import readline from 'node:readline/promises';
import { stdin as input, stdout as output } from 'node:process';
import { request } from './api.js';
import { initConfig } from './init.js';

async function prompt(question) {
  const rl = readline.createInterface({ input, output });
  const answer = await rl.question(question);
  rl.close();
  return answer;
}

export async function actionInit() {
  await initConfig();
}

export async function healthCheck() {
  const text = await request('GET', '/health');
  console.log(text);
}

export async function listTables() {
  const text = await request('GET', '/tables');
  console.log(text);
}

export async function describeTable() {
  const name = await prompt('Table name: ');
  const text = await request('GET', `/tables/${name}`);
  console.log(text);
}

export async function createTable() {
  const path = await prompt('Path to table config JSON: ');
  const fs = await import('fs');
  const body = JSON.parse(fs.readFileSync(path, 'utf8'));
  const text = await request('POST', '/tables', body);
  console.log(text);
}

export async function deleteTable() {
  const name = await prompt('Table name: ');
  const text = await request('DELETE', `/tables/${name}`);
  console.log(text);
}

export async function listSchemas() {
  const text = await request('GET', '/schemas');
  console.log(text);
}

export async function addSchema() {
  const path = await prompt('Path to schema JSON: ');
  const fs = await import('fs');
  const body = JSON.parse(fs.readFileSync(path, 'utf8'));
  const text = await request('POST', '/schemas', body);
  console.log(text);
}

export async function runQuery() {
  const sql = await prompt('SQL query: ');
  const text = await request('POST', '/query/sql', { sql });
  console.log(text);
}

export async function listSegments() {
  const name = await prompt('Table name: ');
  const text = await request('GET', `/tables/${name}/segments`);
  console.log(text);
}

export async function reloadTable() {
  const name = await prompt('Table name: ');
  const text = await request('POST', `/tables/${name}/reload`);
  console.log(text);
}
