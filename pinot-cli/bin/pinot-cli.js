#!/usr/bin/env node
import { Command } from 'commander';
import React from 'react';
import { render } from 'ink';
import Menu from '../src/Menu.js';
import { initConfig } from '../src/init.js';

const program = new Command();

program
  .name('pinot-cli')
  .description('Interactive CLI for Apache Pinot')
  .version('0.1.0');

program
  .command('init')
  .description('Configure base URL and auth token')
  .action(async () => {
    await initConfig();
  });

program
  .command('menu')
  .description('Start interactive menu')
  .action(() => {
    render(React.createElement(Menu));
  });

program.parse(process.argv);

if (process.argv.length <= 2) {
  render(React.createElement(Menu));
}
