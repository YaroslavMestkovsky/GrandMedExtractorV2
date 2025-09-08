  (() => {
    function getDownloadDir() {
      try { return String(window.__DOWNLOAD_DIR || ''); } catch (e) { return ''; }
    }
    function getFilenameFallback(originalFullPath) {
      try {
        const filename = String(window.__FILENAME || '');
        if (filename) return filename;
      } catch (e) {}
      const base = String(originalFullPath || '').split(/[\\/]/).pop();
      return base || 'download';
    }
    function joinPath(base, filename) {
      return String(base || '').replace(/\\+$/, '') + '\\' + String(filename || 'download');
    }

    const origSend = WebSocket.prototype.send;
    WebSocket.prototype.send = function (data) {
      try {
        let txt = data;
        if (data instanceof ArrayBuffer) {
          txt = new TextDecoder('utf-8').decode(new Uint8Array(data));
        } else if (ArrayBuffer.isView(data)) {
          txt = new TextDecoder('utf-8').decode(data);
        } else if (typeof data !== 'string') {
          txt = String(data);
        }

        try {
          const msg = JSON.parse(txt);
          if (msg && msg.Action === 'useraction' && typeof msg.action === 'string') {
            try {
              const inner = JSON.parse(msg.action);
              if (msg.path === '_Writefileend') {
                if (inner.SuccessAction) {
                  inner.SuccessAction = '';
                  msg.action = JSON.stringify(inner);
                  txt = JSON.stringify(msg);
                }
              }
            } catch (e) {}
          }
        } catch (e) {}

        return origSend.call(this, txt);
      } catch (e) {
        try { return origSend.call(this, data); } catch (_) { throw e; }
      }
    };

    const origAddEventListener = WebSocket.prototype.addEventListener;
    WebSocket.prototype.addEventListener = function(type, listener, options) {
      if (type === 'message') {
        const wrapped = function(event) {
          try {
            let data = event.data;
            if (typeof data === 'string') {
              const parsed = JSON.parse(data);
              if (Array.isArray(parsed)) {
                let changed = false;
                for (const item of parsed) {
                  if (item && item.Act === 'DO' && item.Fn === 'FileFastSave' && Array.isArray(item.Pars) && item.Pars.length > 1) {
                    try {
                      const fullPath = String(item.Pars[1] || '');
                      const baseName = getFilenameFallback(fullPath);
                      item.Pars[1] = joinPath(getDownloadDir(), baseName);
                      changed = true;
                    } catch (e) {}
                  }
                }
                if (changed) {
                  const newData = JSON.stringify(parsed);
                  const newEvent = new MessageEvent('message', { data: newData });
                  return listener.call(this, newEvent);
                }
              }
            }
          } catch (e) {}
          return listener.call(this, event);
        };
        return origAddEventListener.call(this, type, wrapped, options);
      }
      return origAddEventListener.call(this, type, listener, options);
    };

    const desc = Object.getOwnPropertyDescriptor(WebSocket.prototype, 'onmessage');
    if (desc && desc.configurable) {
      Object.defineProperty(WebSocket.prototype, 'onmessage', {
        set(handler) {
          const wrapped = function(event) {
            try {
              let data = event.data;
              if (typeof data === 'string') {
                const parsed = JSON.parse(data);
                if (Array.isArray(parsed)) {
                  let changed = false;
                  for (const item of parsed) {
                    if (item && item.Act === 'DO' && item.Fn === 'FileFastSave' && Array.isArray(item.Pars) && item.Pars.length > 1) {
                      try {
                        const fullPath = String(item.Pars[1] || '');
                        const baseName = getFilenameFallback(fullPath);
                        item.Pars[1] = joinPath(getDownloadDir(), baseName);
                        changed = true;
                      } catch (e) {}
                    }
                  }
                  if (changed) {
                    const newEvent = new MessageEvent('message', { data: JSON.stringify(parsed) });
                    return handler.call(this, newEvent);
                  }
                }
              }
            } catch (e) {}
            return handler.call(this, event);
          };
          return desc.set.call(this, wrapped);
        }
      });
    }
  })();
