const { test, expect } = require('@playwright/test');

// Opens a WebSocket from the page and collects what happens until it closes.
function exchange(page, path, messagesToSend) {
  return page.evaluate(([path, messagesToSend]) => new Promise((resolve, reject) => {
    const socket = new WebSocket(location.origin.replace(/^http/, 'ws') + path);
    const received = [];
    const pending = [...messagesToSend];

    socket.onmessage = (event) => {
      received.push(event.data);
      if (pending.length > 0) {
        socket.send(pending.shift());
      } else {
        socket.close(1000);
      }
    };
    socket.onclose = (event) => resolve({ received, code: event.code });
    setTimeout(() => reject(new Error('timed out')), 10000);
  }), [path, messagesToSend]);
}

test('a WebSocket opens through PHP, echoes and closes', async ({ page }) => {
  await page.goto('/');

  const { received, code } = await exchange(page, '/ws', ['hello']);

  const opened = JSON.parse(received[0]);
  expect(opened.connection).toMatch(/^[A-Z2-7]+\.[A-Z2-7]+$/);
  // The stack enables WebSockets over HTTP/2 (GODEBUG=http2xconnect=1), and the
  // browsers use them on the HTTP/2 connection the page came over.
  expect(opened.protocol).toBe('HTTP/2.0');
  expect(received[1]).toBe('echo:hello');
  expect(code).toBe(1000);
});

test('a handshake PHP refuses does not open', async ({ page }) => {
  await page.goto('/');

  const { received, code } = await exchange(page, '/ws?access=denied', []);

  expect(received).toEqual([]);
  expect(code).toBe(1006);
});
