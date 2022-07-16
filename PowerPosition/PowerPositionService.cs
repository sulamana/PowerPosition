using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Configuration;
using System.Data;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.ServiceProcess;
using System.Text;
using System.Timers;
using System.Threading.Tasks;

using Services; //PowerService Objects
//installutil PowerPosition.exe

namespace PowerPosition
{
    public partial class PowerPositionService : ServiceBase
    {

        private EventLog eventlogger = new EventLog() { Source = "PowerPositionService", Log = "Application" };

        private System.Timers.Timer timer = null;       //Timer
        private int TimerInterval = 3600000;            //Interval 1 hour default
        private int MaxRetryAttempts = 5;               //Retry max 5 times Default
        private int RetryDelayInterval = 10000;         //Retry delay interval 10 seconds Default
        private string OutputFolder = ".\\";            //Default current folder
        private bool StartTimer = true;                 //Start the timer after aggregation
        public PowerPositionService()
        {
            InitializeComponent();
            if (!System.Diagnostics.EventLog.SourceExists(eventlogger.Source))
            {
                System.Diagnostics.EventLog.CreateEventSource(eventlogger.Source, eventlogger.Log);
            }
        }
        protected override void OnStart(string[] args)
        {
            try
            {
                // Load config file settings and log the values
                eventlogger.WriteEntry("Start Loading configuration file", EventLogEntryType.Information);

                TimerInterval = int.Parse(ConfigurationManager.AppSettings["TimerInterval"]);
                eventlogger.WriteEntry(string.Format("configuration file TimerInterval = {0}", TimerInterval), EventLogEntryType.Information);

                RetryDelayInterval = int.Parse(ConfigurationManager.AppSettings["RetryDelayInterval"]);
                eventlogger.WriteEntry(string.Format("configuration file RetryDelayInterval = {0}", RetryDelayInterval), EventLogEntryType.Information);

                MaxRetryAttempts = int.Parse(ConfigurationManager.AppSettings["MaxRetryAttempts"]);
                eventlogger.WriteEntry(string.Format("configuration file MaxRetryAttempts = {0}", MaxRetryAttempts), EventLogEntryType.Information);

                OutputFolder = ConfigurationManager.AppSettings["OutputFolder"];
                eventlogger.WriteEntry(string.Format("configuration file OutputFolder = {0}", OutputFolder), EventLogEntryType.Information);

                //Run The Aggregator before starting timer
                RunAggregratePowerPositions();

                //Setup Timer and start if aggregrator finished succesfully
                if (StartTimer)
                {
                    timer = new System.Timers.Timer();
                    timer.Interval = TimerInterval;     // required interval from file
                    timer.AutoReset = false;            // don't keep running timer
                    timer.Enabled = true;               // timer is enabled
                    timer.Elapsed += Timer_Elapsed;     // delegate method to run
                    timer.Start();                      // start the timer
                }
            }
            catch (Exception ex)
            {
                eventlogger.WriteEntry(string.Format("OnStart Failed with following error{0}{1}", Environment.NewLine, ex.Message), EventLogEntryType.Error);
            }
        }
        protected override void OnStop()
        {
            try
            {
                timer.Stop();           //  Stop the timer
                timer.Enabled = false;  // Disable timer
                timer = null;           // Delete timer
            }
            catch (Exception ex)
            {
                eventlogger.WriteEntry(string.Format("OnStop Failed with following error{0}{1}", Environment.NewLine, ex.Message), EventLogEntryType.Error);
            }
        }
        private void Timer_Elapsed(object sender, ElapsedEventArgs e)
        {
            try
            {
                RunAggregratePowerPositions();
            }
            catch (Exception ex)
            {
                eventlogger.WriteEntry(string.Format("RunAggregratePowerPositions Failed with following error{0}{1}", Environment.NewLine, ex.Message), EventLogEntryType.Error);
                StartTimer = false;
            }
            finally
            {
                //If no errors then start timer again. it stops after each run
                if (StartTimer)
                {
                    timer.Start();
                }
            }
        }
        private async void RunAggregratePowerPositions()
        {
            //Run the aggregator and return if we need to start timer 
            StartTimer = await AggregratePowerPositionsAsync();

            //Stop the service if we should not start timer
            if (!StartTimer)
            {
                base.Stop();    //Stop the service
            }
        }
        private async Task<bool> AggregratePowerPositionsAsync()
        {
            eventlogger.WriteEntry(String.Format("Aggregate Power Positions Async @ {0}", DateTime.Now.ToString()), EventLogEntryType.Information);

            bool returnValue = false;
            int attempts = 1;
            Dictionary<int, double> volumes = new Dictionary<int, double>();

            //We will attempt to generate new file MaxRetryAttempts if any errors waiting 10 secs before each retry
            //
            while ((attempts <= MaxRetryAttempts) && (!returnValue))
            {
                try
                {
                    //Get the trades async from the PowerService call
                    PowerService ps = new PowerService();
                    var results = (await ps.GetTradesAsync(DateTime.Now)).ToList(); ;
                    if ((results != null) && (results.Count > 0))
                    {
                        //Aggregrate the volumes
                        foreach (var trade in results)
                        {
                            foreach (var period in trade.Periods)
                            {
                                if (volumes.ContainsKey(period.Period))
                                {
                                    volumes[period.Period] += period.Volume;
                                }
                                else
                                {
                                    volumes.Add(period.Period, period.Volume);
                                }
                            }
                        }

                        // generate new csv filename
                        string csvFilename = string.Format("{0}PowerPosition_{1}.csv", OutputFolder, results[0].Date.ToString("yyyyMMdd_HHmm"));
                        if (System.IO.File.Exists(csvFilename)) System.IO.File.Delete(csvFilename);

                        //Open the CSV file and write contents
                        using (var w = new StreamWriter(csvFilename))
                        {
                            w.WriteLine("Local Time,Volume");
                            for (int i = 1; i <= 24; i++)
                            {
                                string strTime = (i == 1 ? 23 : (i - 2)).ToString().PadLeft(2, '0') + ":00";
                                if (volumes.ContainsKey(i))
                                    w.WriteLine(string.Format("{0},{1}", strTime, volumes[i].ToString()));
                                else
                                    w.WriteLine(string.Format("{0},0", strTime));
                            }
                        }

                        returnValue = true; //Signal we have results and continue processing

                        eventlogger.WriteEntry(string.Format("Successfully generated {0}", csvFilename));
                    }
                    else
                    {
                        await Task.Delay(RetryDelayInterval); //No Data so Wait for retry delay to compplete
                    }
                }
                catch (Exception ex)
                {
                    eventlogger.WriteEntry(string.Format("AggregratePowerPositionsAsync Attempt #{0} - Failed with following error{1}{2}", attempts, Environment.NewLine, ex.Message), EventLogEntryType.Warning);
                    if (attempts > MaxRetryAttempts)
                        eventlogger.WriteEntry(string.Format("AggregratePowerPositionsAsync Exceeded {0} retry attempts", attempts), EventLogEntryType.Error);
                    else
                        await Task.Delay(10000);    //Wait 10 seconds before next attempt
                }

                ++attempts;
            }
            return returnValue;
        }

    }   //End Class
}   //End namespace
