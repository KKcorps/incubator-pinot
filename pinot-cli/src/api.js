import fetch from 'node-fetch';
import { loadConfig } from './config.js';

function headers(config) {
  const h = { 'Content-Type': 'application/json' };
  if (config.token) {
    h['Authorization'] = `Bearer ${config.token}`;
  }
  return h;
}

export async function request(method, path, body) {
  const config = loadConfig();
  const res = await fetch(`${config.baseUrl}${path}`, {
    method,
    headers: headers(config),
    body: body ? JSON.stringify(body) : undefined
  });
  const text = await res.text();
  return text;
}
