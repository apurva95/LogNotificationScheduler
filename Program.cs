using LogNotificationScheduler;

class Program
{
    private static readonly NotificationScheduler scheduler = new();

    static void Main()
    {
        // Create a Timer object
        var timer = new Timer(
            _ => scheduler.RunScheduler(),
            null,
            Timeout.InfiniteTimeSpan, // Initial delay before the first run (no delay in this case)
            TimeSpan.FromMinutes(5) // Interval between subsequent runs
        );

        // Schedule the subsequent runs at 5-minute intervals
        void ScheduleNextRun()
        {
            // Stop the timer before changing its due time
            timer.Change(Timeout.InfiniteTimeSpan, TimeSpan.FromMilliseconds(-1));

            // Schedule the next run after 5 minutes
            timer.Change(TimeSpan.FromMinutes(5), TimeSpan.FromMilliseconds(-1));
        }

        // Start the initial run
        scheduler.RunScheduler();

        // Schedule subsequent runs
        ScheduleNextRun();

        Console.WriteLine("Scheduler started. Press any key to exit.");
        Console.ReadLine();

        // Stop the scheduler and dispose the timer when the application is shutting down
        timer.Dispose();
    }
}