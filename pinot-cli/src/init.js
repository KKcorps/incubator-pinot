import readline from 'node:readline/promises';
import { stdin as input, stdout as output } from 'node:process';
import { loadConfig, saveConfig, configPath } from './config.js';

export async function initConfig() {
  const rl = readline.createInterface({ input, output });
  const current = loadConfig();
  const url = await rl.question(`Controller URL [${current.baseUrl}]: `);
  const token = await rl.question(`Auth token (leave empty if none): `);
  rl.close();
  const config = {
    baseUrl: url || current.baseUrl,
    token: token || ''
  };
  saveConfig(config);
  console.log(`Configuration saved to ${configPath}`);
}
