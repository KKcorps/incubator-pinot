import fs from 'fs';
import os from 'os';
import path from 'path';

export const configPath = path.join(os.homedir(), '.pinot-cli.json');

export function loadConfig() {
  try {
    return JSON.parse(fs.readFileSync(configPath, 'utf8'));
  } catch {
    return { baseUrl: 'http://localhost:9000', token: '' };
  }
}

export function saveConfig(config) {
  fs.writeFileSync(configPath, JSON.stringify(config, null, 2));
}
