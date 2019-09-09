using System;
using System.Timers;
using Npgsql;

namespace Purch_Confirm_server
{
    public class Conf_serv
    {
        readonly Timer _timer;
        NpgsqlConnectionStringBuilder qw = new NpgsqlConnectionStringBuilder()
        {
            Host = "10.0.1.29",
            Port = 5432,
            ConnectionIdleLifetime = 20,
            ApplicationName = "CONFIRM_SERVICE",
            Username = "USER",
            Password = "pass",
            Database = "zakupy"
        };
        public Conf_serv()
        {
            string npA = qw.ToString();
            _timer = new Timer(5000) { AutoReset = true };
            _timer.Elapsed += (sender, eventArgs) =>
            {
                try
                {
                    int ser_run,cnt_serw = 0;
                    using (NpgsqlConnection conA = new NpgsqlConnection(npA))
                    {
                        conA.Open();
                        using (NpgsqlCommand cmd = new NpgsqlCommand("SELECT count(application_name) il FROM pg_catalog.pg_stat_activity where application_name='CONFIRM_SERVICE'", conA))
                        {
                           cnt_serw = Convert.ToInt32(cmd.ExecuteScalar());
                        }
                        if (cnt_serw < 2)
                        {
                            using (NpgsqlCommand cmd = new NpgsqlCommand("Select case when d.state='STOP' then 0 else case when b.in_progress=true then case when b.start_update-interval '5 minutes'<current_timestamp then 0 else case when c.il>0 then case when a.start_update+interval '4 minutes' < current_timestamp then 1 else 0 end	else case when a.start_update+interval '1 hour' < current_timestamp then 1 else 0 end end end else case when c.il>0 then case when a.start_update+interval '4 minutes' < current_timestamp then 1 else 0 end else case when a.start_update+interval '1 hour' < current_timestamp then 1 else 0 end end end end stat from (select start_update from datatbles where table_name='server_progress') a,(select in_progress,start_update from datatbles where table_name='ifs_CONN') b,(SELECT count(application_name) il FROM pg_catalog.pg_stat_activity where application_name='CLIENT') c,(SELECT typ ,case when current_timestamp between (current_date + cast(to_char(start_idle,'HH24:MI:SS') as time)-interval '5 minutes') and (current_date + cast(to_char(stop_idle,'HH24:MI:SS') as time)+interval '5 minutes' ) then 'STOP' else  'RUN' end state from serv_idle where current_timestamp<=current_date + cast(to_char(stop_idle,'HH24:MI:SS') as time)+interval '15 minutes'  order by current_date + cast(to_char(stop_idle,'HH24:MI:SS') as time) limit 1 ) d", conA))
                            {
                                ser_run = Convert.ToInt32(cmd.ExecuteScalar());
                            }
                        }
                        else
                        {
                            ser_run = 0;
                        }                     
                    }
                    if (ser_run > 0)
                    {
                        _timer.Stop();
                        using (Server srv = new Purch_Confirm_server.Server())
                        {
                            srv.Run();
                        }
                        _timer.Start();
                        GC.Collect();
                    }
                }
                catch (Exception e)
                {
                    Console.WriteLine(e);                    
                }
            };

        }
        public void Start() { _timer.Start(); }
        public void Stop()
        {
            string npA = qw.ToString();
            using (NpgsqlConnection conA = new NpgsqlConnection(npA))
            {
                conA.Open();
                using (NpgsqlCommand cmd = new NpgsqlCommand("select cast(count(table_name) as integer) busy  from datatbles where table_name='server_progress' and in_progress=true", conA))
                {
                    int busy_il = 1;
                    while (busy_il > 0)
                    {
                        busy_il = Convert.ToInt16(cmd.ExecuteScalar());
                        if (busy_il > 0) { System.Threading.Thread.Sleep(1000); }
                    }
                }              
            }
            _timer.Stop();
        }

    }
}
