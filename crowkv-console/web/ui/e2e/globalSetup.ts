import { consoleBaseURL } from './fixtures/realBackend';
import { resetAll } from './fixtures/consoleSetup';

export default async function globalSetup() {
  const baseURL = consoleBaseURL();
  await resetAll(baseURL);
}
