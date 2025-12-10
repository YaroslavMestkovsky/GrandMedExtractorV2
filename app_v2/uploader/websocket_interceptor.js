(() => {
    function extractDownloadParams(item) {
      try {
        if (item && item.Act === 'DO' && item.Fn === 'FileFastSave' && Array.isArray(item.Pars) && item.Pars.length > 0) {
          const mtempPrtCall = String(item.Pars[0] || '');
          const match = mtempPrtCall.match(/mtempPrt\((\d+),"(.*?)",(\d+),"(.*?)","(.*?)","(.*?)"\)/);
          if (match) {
            return {
              report_id: parseInt(match[1]),
              report_type: match[2],
              mode: parseInt(match[3]),
              body: match[4],
              fmt: match[5],
              layout: match[6],
              full_path: mtempPrtCall
            };
          }
        }
      } catch (e) {}
      return null;
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
                  if (item && item.Act === 'DO' && item.Fn === 'FileFastSave' && Array.isArray(item.Pars) && item.Pars.length > 0) {
                    try {
                      const downloadParams = extractDownloadParams(item);
                      if (downloadParams) {
                        window.__DOWNLOAD_PARAMS = downloadParams;
                        console.log('Download params extracted from incoming message:', downloadParams);
                      }
                      if (item.Pars.length > 1) {
                        let sink = '';
                        try { sink = String(window.__BLACKHOLE_PATH || ''); } catch (e) {}
                        if (!sink) sink = 'C\\\Windows\\\Temp\\\qms_discard.tmp';
                        item.Pars[1] = sink;
                      }
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
                    if (item && item.Act === 'DO' && item.Fn === 'FileFastSave' && Array.isArray(item.Pars) && item.Pars.length > 0) {
                      try {
                        const downloadParams = extractDownloadParams(item);
                        if (downloadParams) {
                          window.__DOWNLOAD_PARAMS = downloadParams;
                          console.log('Download params extracted from incoming message:', downloadParams);
                        }
                        if (item.Pars.length > 1) {
                          let sink = '';
                          try { sink = String(window.__BLACKHOLE_PATH || ''); } catch (e) {}
                          if (!sink) sink = 'C\\\Windows\\\Temp\\\qms_discard.tmp';
                          item.Pars[1] = sink;
                        }
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
