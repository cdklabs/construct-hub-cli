import { hostname, userInfo } from 'os';
import { S3Client, paginateListObjectsV2 } from '@aws-sdk/client-s3';
import { SFNClient, StartExecutionCommand, paginateGetExecutionHistory, HistoryEvent, DescribeExecutionCommand } from '@aws-sdk/client-sfn';
import { Command, flags } from '@oclif/command';
import chalk from 'chalk';
import { Listr, ListrRendererFactory, ListrTask } from 'listr2';
import { Observable, Subscriber } from 'rxjs';
import { v4 as uuidv4, v5 as uuidv5 } from 'uuid';

export default class List extends Command {
  public static readonly description: string = 'Lists packages in a ConstructHub instance';
  public static readonly flags = {
    'package-data': flags.string({
      description: 'The ConstructHub package data bucket name',
      helpValue: 'bucket-name',
      required: true,
    }),
    'fix-using': flags.string({
      description: 'Attempt to fix packages that are missing documentation for one or more languages using the specified SFN State Machine',
      helpValue: 'state-machine-arn',
    }),
    'fix-batches': flags.integer({
      description: 'The number of elements to accumulate before running the SFN State Machine specified by --fix-using',
      default: 25,
    }),
    'fix-wait': flags.boolean({
      description: 'Whether to wait for the StateMachine executions to complete before moving forward',
      default: true,
      allowNo: true,
    }),
    'languages': flags.string({
      description: 'The languages supported by the ConstructHub instance',
      multiple: true,
      default: ['java', 'python', 'typescript'],
    }),
    'help': flags.help({ char: 'h' }),
  };

  public async run() {
    const {
      flags: {
        'fix-batches': fixBatchSize,
        'fix-using': stateMachine,
        'fix-wait': wait,
        languages,
        'package-data': bucketName,
      },
    } = this.parse(List);

    const S3 = new S3Client({});
    const SFN = new SFNClient({});

    const ns = uuidv4();
    const runningMachines = new Map<string, Promise<{ readonly arn: string; readonly status: string }>>();
    const highWaterMark = 50;
    const lowWaterMark = Math.max(0, highWaterMark - fixBatchSize);

    const broken = new Array<PackageVersionStatus>();
    for await (const prefix of this.packageVersionPrefixes(S3, bucketName)) {
      const status = await this.processPackageVersion(S3, bucketName, prefix);
      if (!status.assemblyKey || !status.metadataKey || !status.tarballKey) {
        console.error(`${chalk.blue.bold(status.name)}@${chalk.blue.bold(status.version)}`, chalk.red('has missing base objects'));
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
        broken.push(status);
      }

      if (stateMachine && broken.length >= fixBatchSize) {
        if (runningMachines.size >= highWaterMark) {
          await this.waitForSettlements(runningMachines, lowWaterMark);
        }
        const { executionArns } = await this.repairPackageVersions(SFN, bucketName, languages, stateMachine, broken, wait, ns);
        if (!wait) {
          for (const arn of executionArns.values()) {
            runningMachines.set(arn, this.awaitStateMachineEnd(SFN, arn));
          }
        }
        broken.splice(0, broken.length);
      }
    }

    if (stateMachine) {
      if (runningMachines.size >= highWaterMark) {
        await this.waitForSettlements(runningMachines, lowWaterMark);
      }
      const { executionArns } = await this.repairPackageVersions(SFN, bucketName, languages, stateMachine, broken, wait, ns);
      if (!wait) {
        for (const arn of executionArns.values()) {
          runningMachines.set(arn, this.awaitStateMachineEnd(SFN, arn));
        }
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
    languages: readonly string[],
    stateMachine: string,
    broken: readonly PackageVersionStatus[],
    wait: boolean,
    ns: string,
  ) {
    return new Listr<{ executionArns: Map<string, string> }>(
      broken.map((status) => {
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
                    '#comment': `Triggered by construct-hub-cli by ${userInfo().username} on ${hostname()}`,
                  }, null, 2),
                }));
                if (executionArn) {
                  ctx.executionArns.set(name, executionArn);
                  if (!wait) {
                    fixItemTask.title += ` => ${chalk.yellow(executionArn)}`;
                  }
                }
                return executionArn;
              },
            }, {
              title: 'Monitor State Machine',
              enabled: (ctx) => ctx.executionArns.has(name),
              skip: !wait,
              task: (ctx, monitorTask) => {
                monitorTask.title = `Monitor: ${chalk.blue.underline(`https://console.aws.amazon.com/states/home#/executions/details/${ctx.executionArns.get(name)}`)}`;
                return this.monitorStateMachine(SFN, ctx.executionArns.get(name)!, languages, monitorTask);
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

  private monitorStateMachine<Ctx, Renderer extends ListrRendererFactory>(
    SFN: SFNClient,
    executionArn: string,
    languages: readonly string[],
    parent: Parameters<ListrTask<Ctx, Renderer>['task']>[1],
  ) {
    const status = new Map<string, { readonly observable: Observable<any>; subscriber?: Subscriber<any> }>();
    for (const language of languages) {
      const observable = new Observable<any>((observer) => {
        const entry = status.get(language);
        if (entry) {
          entry.subscriber = observer;
        }
      });
      status.set(language, { observable });
    }

    function settle(language: string, reason?: any) {
      if (!status.has(language)) {
        return;
      }
      const { subscriber } = status.get(language)!;
      if (reason) {
        subscriber?.error(reason);
      } else {
        subscriber?.complete();
      }
      status.delete(language);
    }

    function update(language: string, message: string) {
      if (!status.has(language)) {
        return;
      }
      const { subscriber } = status.get(language)!;
      subscriber?.next(message);
    }

    const tasks = parent.newListr([{
      title: 'Wait for doc-gen to complete',
      task: (_ctx, task) => task.newListr(
        Array.from(status.entries()).map(([language, { observable }]) => ({
          title: language,
          // NOTE: Don't understand why "as any" is needed here. It complains Observable<any> is incomaptible with Observale<any>.
          task: () => observable,
        })),
        { exitOnError: false, concurrent: true }),
    }]);

    setImmediate(trackExecution);

    return tasks;

    function trackExecution() {
      const taskLanguage = new Map<number, string>();

      function fail(cause: any) {
        for (const language of status.keys()) {
          settle(language, cause);
        }
      }

      function handleNewEvents([events, nextSince]: [readonly HistoryEvent[], number]) {
        for (const event of events) {
          if (event.previousEventId && taskLanguage.has(event.previousEventId)) {
            const language = taskLanguage.get(event.previousEventId)!;
            taskLanguage.set(event.id!, language);
            update(language, `[${chalk.green(event.timestamp?.toISOString())}] #${chalk.bold(event.id)} ${chalk.blue(event.type)}`);
          }
          switch (event.type) {
            case 'ExecutionAborted':
              return fail(new Error('State Machine execution aborted'));
            case 'ExecutionFailed':
              return fail(new Error('State Machine execution failed'));
            case 'ExecutionSucceeded':
              return fail(new Error('Execution completed before task finished...'));
            case 'TaskStateEntered':
              const matches = /^Generate ([^\s]+) docs$/.exec(event.stateEnteredEventDetails!.name!);
              if (matches) {
                const [, language] = matches;
                update(language, `[${chalk.green(event.timestamp?.toISOString())}] #${chalk.bold(event.id)} ${chalk.blue(event.type)} => ${chalk.italic(event.stateEnteredEventDetails!.name)}`);
                taskLanguage.set(event.id!, language);
              }
              break;
            case 'TaskSucceeded':
              if (taskLanguage.has(event.id!)) {
                const language = taskLanguage.get(event.id!)!;
                settle(language);
              }
              break;
            case 'TaskStateExited':
              // If not settled yet, then the task ran out of attempts & failed.
              if (taskLanguage.has(event.id!)) {
                const language = taskLanguage.get(event.id!)!;
                settle(language, new Error('Ran out of attempts'));
              }
              break;
          }
        }
        setTimeout(() => newEvents(nextSince).then(handleNewEvents, fail), 1_000);
      };

      async function newEvents(since: number = 0): Promise<[readonly HistoryEvent[], number]> {
        const result = new Array<HistoryEvent>();
        const paginator = paginateGetExecutionHistory({ client: SFN }, { executionArn, includeExecutionData: false, reverseOrder: true });
        let nextSince = since;
        for await (const { events = [] } of paginator) {
          for (const event of events) {
            if (event.id && event.id <= since) {
              return [result.reverse(), nextSince];
            }
            result.push(event);
            if (event.id && event.id > nextSince) {
              nextSince = event.id;
            }
          }
        }
        return [result.reverse(), nextSince];
      }

      newEvents().then(handleNewEvents, fail);
    }
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
