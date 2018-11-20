using System;
using System.IO;

namespace BaseWindowsService
{
    public class FileWatcher :IDisposable
    {

        protected string FileToWatch;

        private FileSystemWatcher watcher;

        public event FileSystemEventHandler OnFileChanged;

        public FileWatcher(string filepath)
        {
            FileToWatch = filepath;
        }

        public void Start(NotifyFilters filter= NotifyFilters.LastWrite)
        {
            Stop();

            watcher = new FileSystemWatcher();
            watcher.Path = Path.GetDirectoryName(FileToWatch) + "\\";
            watcher.Filter = Path.GetFileName(FileToWatch);
            watcher.NotifyFilter = filter;
            watcher.Changed += Watcher_Changed;
            watcher.EnableRaisingEvents = true;
        }

        public void Stop()
        {
            if (watcher != null)
            {
                watcher.EnableRaisingEvents = false;
                watcher.Dispose();
                watcher = null;
            }
        }

        private void Watcher_Changed(object sender, FileSystemEventArgs e)
        {
            if (e.ChangeType == WatcherChangeTypes.Changed)
            {
                OnFileChanged?.Invoke(this, e);
            }
        }

        #region IDisposable Support
        private bool disposedValue = false; 

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    Stop();
                }

                // TODO: free unmanaged resources (unmanaged objects) and override a finalizer below.
                // TODO: set large fields to null.

                disposedValue = true;
            }
        }

        // TODO: override a finalizer only if Dispose(bool disposing) above has code to free unmanaged resources.
        // ~FileWatcher() {
        //   // Do not change this code. Put cleanup code in Dispose(bool disposing) above.
        //   Dispose(false);
        // }

        public void Dispose()
        {
            Dispose(true);
            // TODO: uncomment the following line if the finalizer is overridden above.
            // GC.SuppressFinalize(this);
        }
        #endregion
    }
}
