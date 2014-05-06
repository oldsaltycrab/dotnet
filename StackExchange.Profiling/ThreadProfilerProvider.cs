using System;

namespace StackExchange.Profiling
{
    /// <summary>
    /// Locked to the thread which created it, only an instance of a <see cref="MiniProfiler"/> per thread.
    /// </summary>
    public class ThreadProfilerProvider : IProfilerProvider
    {
        [ThreadStatic]
        private static MiniProfiler _profiler = null;

        /// <summary>
        /// The name says it all
        /// </summary>
        /// <returns></returns>
        public MiniProfiler GetCurrentProfiler()
        {
            return _profiler;
        }

        /// <summary>
        /// Starts a new profiling session.
        /// </summary>
        public MiniProfiler Start(string sessionName = null)
        {
			if (_profiler == null)
			{
				_profiler = new MiniProfiler(sessionName ?? AppDomain.CurrentDomain.FriendlyName + "[" + System.Threading.Thread.CurrentThread.ManagedThreadId + "]") { IsActive = true };
			}
            return _profiler;
        }

        /// <summary>
        /// Starts a new profiling session.
        /// </summary>
        [Obsolete("Please use the Start(string sessionName) overload instead of this one. ProfileLevel is going away.")]
        public MiniProfiler Start(ProfileLevel level, string sessionName = null) 
        {
            return Start(sessionName);
        }

        /// <summary>
        /// Stops the current profiling session.
        /// </summary>
        public void Stop(bool discardResults)
        {
			if (_profiler != null)
			{
				_profiler.StopImpl();

				 // save the profiler
				SaveProfiler();
			}
        }

        /// <summary>
        /// Calls <see cref="MiniProfiler.Settings.EnsureStorageStrategy"/> to save the current
        /// profiler using the current storage settings. 
        /// If <see cref="MiniProfiler.Storage"/> is set, this will be used.
        /// </summary>
        protected static void SaveProfiler()
        {
            // because we fetch profiler results after the page loads, we have to put them somewhere in the meantime
            // If the current MiniProfiler object has a custom IStorage set in the Storage property, use it. Else use the Global Storage.
            var storage = _profiler.Storage;
			if (storage == null)
            {
                storage = MiniProfiler.Settings.Storage;
            }
            if (storage != null)
            {
				storage.Save(_profiler);
            }
        }
    }
}
