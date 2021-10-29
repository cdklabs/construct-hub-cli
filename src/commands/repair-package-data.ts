import { hostname, userInfo } from 'os';
import { Readable } from 'stream';
import { gunzipSync } from 'zlib';
import { S3Client, paginateListObjectsV2, GetObjectCommand } from '@aws-sdk/client-s3';
import { SFNClient, StartExecutionCommand, DescribeExecutionCommand } from '@aws-sdk/client-sfn';
import { defaultProvider } from '@aws-sdk/credential-provider-node';
import { Command, flags } from '@oclif/command';
import chalk from 'chalk';
import { Listr } from 'listr2';
import { v4 as uuidv4, v5 as uuidv5 } from 'uuid';

export default class RepairPackageData extends Command {
  public static readonly description: string = 'Repairs package data in a ConstructHub instance';
  public static readonly flags = {
    'bucket': flags.string({
      description: 'The ConstructHub package data bucket name',
      helpValue: 'bucket-name',
      required: true,
    }),
    'state-machine': flags.string({
      description: 'Attempt to fix packages that are missing documentation for one or more languages using the specified SFN State Machine',
      helpValue: 'arn',
    }),
    'batch-size': flags.integer({
      description: 'The number of elements to accumulate before running the SFN State Machine specified by --fix-using',
      default: 25,
    }),
    'languages': flags.string({
      description: 'The languages supported by the ConstructHub instance',
      multiple: true,
      default: ['csharp', 'java', 'python', 'typescript'],
    }),
    'only-catalog': flags.boolean({
      description: 'Whether to limit the process to versions present in the catalog object',
      default: false,
    }),
    'help': flags.help({ char: 'h' }),
  };

  public async run() {
    const {
      flags: {
        'batch-size': fixBatchSize,
        'only-catalog': onlyCatalog,
        'bucket': bucketName,
        'state-machine': stateMachine,
        languages,
      },
    } = this.parse(RepairPackageData);

    const credentials = defaultProvider({ maxRetries: 10 });

    const S3 = new S3Client({ credentials });
    const SFN = new SFNClient({ credentials });

    const ns = uuidv4();
    const runningMachines = new Map<string, Promise<{ readonly arn: string; readonly status: string }>>();
    const highWaterMark = 50;
    const lowWaterMark = Math.max(0, highWaterMark - fixBatchSize);

    const broken = new Array<[PackageVersionStatus, readonly string[]]>();
    const packageVersionPrefixes = onlyCatalog
      ?this.packageVersionPrefixesFromCatalog(S3, bucketName)
      : this.packageVersionPrefixes(S3, bucketName);
    for await (const prefix of packageVersionPrefixes) {
      const status = await this.processPackageVersion(S3, bucketName, prefix);
      if (!status.assemblyKey || !status.metadataKey || !status.tarballKey) {
        console.error(
          `${chalk.blue.bold(status.name)}@${chalk.blue.bold(status.version)}`,
          chalk.red('has missing base objects'),
          ' - ',
          [
            `assembly: ${status.assemblyKey ? chalk.bgGreen('PRESENT') : chalk.bgRed('MISSING')}`,
            `metadata: ${status.metadataKey ? chalk.bgGreen('PRESENT') : chalk.bgRed('MISSING')}`,
            `tarball: ${status.tarballKey ? chalk.bgGreen('PRESENT') : chalk.bgRed('MISSING')}`,
          ].join(', '),
        );
        continue;
      }
      const missingLanguages = new Set<string>();
      for (const language of languages) {
        if (status.docs.get(language) == null) {
          missingLanguages.add(language);
        }
        for (const subStatus of status.submodules.values()) {
          if (subStatus.get(language) == null) {
            missingLanguages.add(language);
          }
        }
      }
      if (missingLanguages.size > 0) {
        console.log(`${chalk.blue.bold(status.name)}@${chalk.blue.bold(status.version)} is missing documents for ${Array.from(missingLanguages).map((l) => chalk.green(l)).join(', ')}`);
        broken.push([status, Array.from(missingLanguages)]);
      }

      if (stateMachine && broken.length >= fixBatchSize) {
        if (runningMachines.size >= highWaterMark) {
          await this.waitForSettlements(runningMachines, lowWaterMark);
        }
        const { executionArns } = await this.repairPackageVersions(SFN, bucketName, stateMachine, broken, ns);
        for (const arn of executionArns.values()) {
          runningMachines.set(arn, this.awaitStateMachineEnd(SFN, arn));
        }
        broken.splice(0, broken.length);
      }
    }

    if (stateMachine) {
      if (runningMachines.size >= highWaterMark) {
        await this.waitForSettlements(runningMachines, lowWaterMark);
      }
      const { executionArns } = await this.repairPackageVersions(SFN, bucketName, stateMachine, broken, ns);
      for (const arn of executionArns.values()) {
        runningMachines.set(arn, this.awaitStateMachineEnd(SFN, arn));
      }
    }
  }

  private async waitForSettlements(promises: Map<string, Promise<any>>, lowWaterMark: number) {
    console.error(`At ${promises.size} executions... waiting until there are less than ${lowWaterMark}...`);
    while (promises.size > lowWaterMark) {
      const { arn, status } = await Promise.race(promises.values());
      if (status !== 'SUCCEEDED') {
        console.error(`Execution ${arn} finished in ${status}`);
      }
      promises.delete(arn);
    }
  }

  private awaitStateMachineEnd(SFN: SFNClient, executionArn: string): Promise<{readonly arn: string; readonly status: string}> {
    return new Promise((ok, ko) => {
      const command = new DescribeExecutionCommand({ executionArn });
      (function evaluate() {
        SFN.send(command).then(
          result => {
            if (result.status !== 'RUNNING') {
              return ok({ arn: executionArn, status: result.status! });
            }
            // 30 seconds + up to 30 seconds jitter
            setImmediate(evaluate, 30_000 + 30_000 * Math.random());
          },
          (cause) => {
            if (cause.name !== 'ThrottlingException' && cause.name !== 'TimeoutError') {
              return ko(cause);
            }
            setImmediate(evaluate, 30_000 + 30_000 * Math.random());
          },
        );
      })();
    });
  }

  private async repairPackageVersions(
    SFN: SFNClient,
    bucketName: string,
    stateMachine: string,
    broken: readonly [PackageVersionStatus, readonly string[]][],
    ns: string,
  ) {
    return new Listr<{ executionArns: Map<string, string> }>(
      broken.map(([status, missingLanguages]) => {
        const name = `${userInfo().username}-${uuidv5(`${status.name}@${status.version}`, ns)}`;
        return {
          title: `Repairing ${chalk.blue.bold(status.name)}@${chalk.blue.bold(status.version)}`,
          task: (_ctx, fixItemTask) => fixItemTask.newListr([
            {
              title: `Start State Machine (${name})`,
              task: async (ctx) => {
                const { executionArn } = await SFN.send(new StartExecutionCommand({
                  stateMachineArn: stateMachine,
                  name,
                  input: JSON.stringify({
                    'bucket': bucketName,
                    'assembly': { key: status.assemblyKey },
                    'metadata': { key: status.metadataKey },
                    'package': { key: status.tarballKey },
                    'languages': missingLanguages.reduce((selection, lang) => ({ ...selection, [lang]: true }), {}),
                    '#comment': `Triggered by construct-hub-cli by ${userInfo().username} on ${hostname()}`,
                  }, null, 2),
                }));
                if (executionArn) {
                  ctx.executionArns.set(name, executionArn);
                  fixItemTask.title += ` => ${chalk.yellow(executionArn)}`;
                }
                return executionArn;
              },
            },
          ]),
        };
      }), {
        concurrent: 5,
        ctx: { executionArns: new Map<string, string>() },
        exitOnError: false,
      }).run();
  }

  private async *packageVersionPrefixes(S3: S3Client, bucketName: string, prefix = 'data/'): AsyncGenerator<string, void> {
    for await (const { CommonPrefixes: commonPrefixes } of paginateListObjectsV2({ client: S3 }, { Bucket: bucketName, Delimiter: '/', Prefix: prefix })) {
      if (!commonPrefixes) { continue; }
      for (const { Prefix: commonPrefix } of commonPrefixes) {
        if (commonPrefix?.startsWith('data/@') && prefix === 'data/') {
          // This is a scoped package
          yield* this.packageVersionPrefixes(S3, bucketName, commonPrefix);
        } else if (commonPrefix) {
          yield* allVersionsForPrefix(commonPrefix);
        }
      }
    }

    async function* allVersionsForPrefix(packagePrefix: string): AsyncGenerator<string, void> {
      for await (const page of paginateListObjectsV2({ client: S3 }, { Bucket: bucketName, Delimiter: '/', Prefix: packagePrefix })) {
        if (!page.CommonPrefixes) { continue; }
        for (const { Prefix: versionPrefix } of page.CommonPrefixes) {
          if (versionPrefix) {
            yield versionPrefix;
          }
        }
      }
    }
  }

  private async *packageVersionPrefixesFromCatalog(S3: S3Client, bucketName: string): AsyncGenerator<string, void> {
    const { Body, ContentEncoding } = await S3.send(new GetObjectCommand({ Bucket: bucketName, Key: 'catalog.json' }));
    if (!Body) {
      return;
    }

    const buffer = await new Promise<any>(async (ok, ko) => {
      if (isBlob(Body)) {
        const buff = await Body.arrayBuffer();
        ok(Buffer.from(buff));
      } else if (Body instanceof Readable) {
        Body.once('error', ko);
        const chunks = new Array<Buffer>();
        Body.once('close', () => {
          try {
            ok(Buffer.concat(chunks));
          } catch (err) {
            ko(err);
          }
        });
        Body.on('data', (chunk) => chunks.push(Buffer.from(chunk)));
      } else {
        // Cannot be a Blob, as at this stage we established the Blob type does not even exist...
        const reader = Body.getReader();
        let result: Awaited<ReturnType<typeof reader.read>>;
        const chunks = new Array<Buffer>();
        do {
          result = await reader.read();
          if (result.value) {
            chunks.push(Buffer.from(result.value));
          }
        } while (!result.done);
        ok(Buffer.concat(chunks));
      }
    });

    const catalog = JSON.parse(
      (
        ContentEncoding === 'gzip'
          ? gunzipSync(buffer)
          : buffer
      ).toString('utf-8'),
    );

    for (const { name, version } of catalog.packages) {
      yield `data/${name}/v${version}/`;
    }
  }

  private async processPackageVersion(S3: S3Client, bucketName: string, prefix: string): Promise<PackageVersionStatus> {
    const [, name, version] = /^data\/((?:@[^/]+\/)?[^/]+)\/v([^/]+)\/$/.exec(prefix)!;
    const result = {
      name,
      version,
      assemblyKey: undefined as string | undefined,
      metadataKey: undefined as string | undefined,
      tarballKey: undefined as string | undefined,
      docs: new Map<string, DocStatus>(),
      submodules: new Map<string, Map<string, DocStatus>>(),
    };

    for await (const { Contents: objects } of paginateListObjectsV2({ client: S3 }, { Bucket: bucketName, Prefix: prefix })) {
      if (!objects) { continue; }
      let matches: RegExpMatchArray | null = null;
      for (const { Key: key } of objects) {
        if (!key) { continue; }
        if (key.endsWith('/assembly.json')) {
          result.assemblyKey = key;
        } else if (key.endsWith('/metadata.json')) {
          result.metadataKey = key;
        } else if (key.endsWith('/package.tgz')) {
          result.tarballKey = key;
        } else if ((matches = /^.*\/docs-([^-]+-)?([a-z]+)\.md(\.not-supported)?$/.exec(key)) != null) {
          const [, submodule, lang, unsupported] = matches;
          const status = unsupported ? DocStatus.UNSUPPORTED : DocStatus.PRESENT;
          if (submodule) {
            if (!result.submodules.has(submodule)) {
              result.submodules.set(submodule, new Map());
            }
            result.submodules.get(submodule)!.set(lang, status);
          } else {
            result.docs.set(lang, status);
          }
        } else {
          console.error(`Unexpected key could not be attributed to any document category: ${key}`);
          process.exit(-1);
        }
      }
    }

    return result;
  }
}

const enum DocStatus {
  PRESENT = 'Present',
  UNSUPPORTED = 'Unsupported',
}

interface PackageVersionStatus {
  readonly name: string;
  readonly version: string;

  readonly assemblyKey?: string;
  readonly metadataKey?: string;
  readonly tarballKey?: string;

  readonly docs: ReadonlyMap<string, DocStatus>;
  readonly submodules: ReadonlyMap<string, ReadonlyMap<string, DocStatus>>;
}

type Awaited<T> = T extends Promise<infer A> ? A : T;

function isBlob(val: any): val is Blob {
  try {
    return val instanceof Blob;
  } catch {
    // We may receive "ReferenceError: Blob is not defined", in which case it CANNOT possibly be a Blob.
    return false;
  }
}
