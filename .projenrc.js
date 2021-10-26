const { TypeScriptAppProject } = require('projen');

const project = new TypeScriptAppProject({
  name: 'construct-hub-cli',
  license: 'Apache-2.0',
  defaultReleaseBranch: 'main',
  minNodeVersion: '12.4.0',
  workflowNodeVersion: '12.x',

  tsconfig: {
    compilerOptions: {
      importHelpers: true,
      lib: ['es2019', 'dom'],
    },
  },

  bin: {
    'construct-hub': './bin/run',
  },

  scripts: {
    prepack: 'rm -rf lib && tsc -b && oclif-dev manifest && oclif-dev readme',
    postpack: 'rm -f oclif.manifest.json',
  },

  deps: [
    '@aws-sdk/client-s3',
    '@aws-sdk/client-sfn',
    '@oclif/command',
    '@oclif/config',
    '@oclif/plugin-help',
    'chalk',
    'listr2',
    'rxjs',
    'tslib',
    'uuid',
  ],

  devDeps: [
    '@oclif/dev-cli',
    '@types/chalk',
    '@types/uuid',
  ],

  npmignoreEnabled: false,
});

project.package.addField('oclif', {
  commands: './lib/commands',
  bin: 'construct-hub',
  plugins: ['@oclif/plugin-help'],
});

project.package.addField('files', [
  '/bin',
  '/lib',
  '/LICENSE',
  '/NOTICE',
  '/README.md',
]);

project.synth();
