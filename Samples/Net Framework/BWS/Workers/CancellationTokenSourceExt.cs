using System;
using System.Threading;

namespace BaseWindowsService.Workers
{
    public class CancellationTokenSourceExt : CancellationTokenSource
    {
        public event EventHandler OnCancel;

        public CancellationTokenSourceExt() : base() { }

        public CancellationTokenSourceExt(TimeSpan delay) : base(delay)
        {
        }

        public CancellationTokenSourceExt(int millisecondsDelay) : base(millisecondsDelay)
        {
        }

        public new void Cancel()
        {
            base.Cancel();
            OnCancel?.Invoke(this, EventArgs.Empty);
        }
    }
}
