using Npgsql;
using Oracle.ManagedDataAccess.Client;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Net.Mail;
using System.Threading;
using System.Threading.Tasks;


namespace Purch_Confirm_server
{
    public static class SendMailEx
    {
        public static Task SendMailExAsync(
            this System.Net.Mail.SmtpClient @this,
            System.Net.Mail.MailMessage message,
            CancellationToken token = default(CancellationToken))
        {
            // use Task.Run to negate SynchronizationContext
            return Task.Run(() => SendMailExImplAsync(@this, message, token));
        }

        private static async Task SendMailExImplAsync(
            System.Net.Mail.SmtpClient client,
            System.Net.Mail.MailMessage message,
            CancellationToken token)
        {
            token.ThrowIfCancellationRequested();

            var tcs = new TaskCompletionSource<bool>();
            System.Net.Mail.SendCompletedEventHandler handler = null;
            void unsubscribe() => client.SendCompleted -= handler;
            handler = async (s, e) =>
            {
                unsubscribe();
                await Task.Yield();
                if (e.UserState != tcs)
                    tcs.TrySetException(new InvalidOperationException("Unexpected UserState"));
                else if (e.Cancelled)
                    tcs.TrySetCanceled();
                else if (e.Error != null)
                    tcs.TrySetException(e.Error);
                else
                    tcs.TrySetResult(true);
            };

            client.SendCompleted += handler;
            try
            {
                client.SendAsync(message, tcs);
                using (token.Register(() => client.SendAsyncCancel(), useSynchronizationContext: false))
                {
                    await tcs.Task;
                }
            }
            finally
            {
                unsubscribe();
            }
        }
    }
    class Server : IDisposable // moduł główny wyliczający problemy materiałowe
    {
        static TaskScheduler main_cal = TaskScheduler.Default;
        private readonly ParallelOptions srv_op = new ParallelOptions
        {
            CancellationToken = CancellationToken.None,
            MaxDegreeOfParallelism = 100,
            TaskScheduler = main_cal
        };
        private bool isDisposed = false;
        public string serv_state = "Ready";
        protected void Dispose(bool disposing)
        {
            if (!isDisposed)
            {
                if (disposing)
                {
                    StMag.Clear();
                    DMND_ORA.Clear();
                    DEMANDS.Clear();
                    POTW.Clear();
                    dta.Clear();
                    TASK_DEMANDS.Clear();
                    TMPTASK_dat.Clear();
                    MShedul.Clear();
                    eras_Shedul.Clear();
                    StMag.Dispose();
                    DMND_ORA.Dispose();
                    DEMANDS.Dispose();
                    POTW.Dispose();
                    dta.Dispose();
                    TASK_DEMANDS.Dispose();
                    TASK_dat.Dispose();
                    TMPTASK_dat.Dispose();
                    MShedul.Dispose();
                    eras_Shedul.Dispose();

                    // tu zwalniamy zasoby zarządzane (standardowe klasy)                    
                }

                // tu zwalniamy zasoby niezarządzane (np. strumienie, obiekty COM itp.)
            }
            this.isDisposed = true;
        }
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }
        public DateTime Serw_run = DateTime.Now;
        public string Log_rek = "Logs Started at " + DateTime.Now;
        private void Log(string txt)
        {
            Log_rek = Log_rek + Environment.NewLine + txt;
        }
        public string HTMLEncode(string text)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder();
            foreach (char c in text)
            {
                if (c > 122) // special chars
                    sb.Append(String.Format("&#{0};", (int)c));
                else
                if (c == 47)//|| c >57 & c < 65 || c > 90 & c < 97)
                {
                    sb.Append(String.Format("&#{0};", (int)c));
                }
                else
                {
                    sb.Append(c);
                }
            }
            return sb.ToString();
        }
        private string TD(String strIn, Boolean oldco = false, Boolean bold = false, Boolean nowrap = false)
        {
            char newlin = (Char)13;
            string a1 = "";
            string a2 = "";
            string a3 = "";
            if (oldco) { a1 = a1 + " bgcolor=" + (Char)34 + "yellow" + (Char)34; }
            if (bold & oldco)
            {
                a2 = "<b>";
                a3 = "</b>";
            }
            if (nowrap) { a1 = a1 + " nowrap=" + (Char)34 + "nowrap" + (Char)34; }
            a1 = a1 + ">";
            return "<td" + a1 + a2 + strIn.Replace("00:00:00", "").Replace(newlin.ToString(), "<br />") + a3 + " </td>";
        }
        private void Save_stat_refr()
        {
            try
            {
                NpgsqlConnectionStringBuilder qw = new NpgsqlConnectionStringBuilder()
                {
                    Host = "10.0.1.29",
                    Port = 5432,
                    ConnectionIdleLifetime = 20,
                    ApplicationName = "ZAKUPY_CONNECTOR",
                    Username = "USER",
                    Password = "pass",
                    Database = "zakupy"
                };
                string npA = qw.ToString();
                using (NpgsqlConnection conA = new NpgsqlConnection(npA))
                {
                    conA.Open();
                    using (NpgsqlTransaction tr_savelogs = conA.BeginTransaction())
                    {
                        using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                            "INSERT INTO public.server_query" +
                            "(start_date, end_dat, errors_found, log, id) " +
                            "VALUES" +
                            "(@run,@end,@er,@log,@id); ", conA))
                        {
                            cmd.Parameters.Add("run", NpgsqlTypes.NpgsqlDbType.Timestamp);
                            cmd.Parameters.Add("end", NpgsqlTypes.NpgsqlDbType.Timestamp);
                            cmd.Parameters.Add("er", NpgsqlTypes.NpgsqlDbType.Integer);
                            cmd.Parameters.Add("log", NpgsqlTypes.NpgsqlDbType.Text);
                            cmd.Parameters.Add("id", NpgsqlTypes.NpgsqlDbType.Uuid);
                            cmd.Prepare();
                            string searchTerm = "Błąd";
                            string[] source = Log_rek.Split(new char[] { '.', '?', '!', ' ', ';', ':', ',', '\n' }, StringSplitOptions.RemoveEmptyEntries);
                            var matchQuery = from word in source
                                             where word.ToLowerInvariant() == searchTerm.ToLowerInvariant()
                                             select word;
                            cmd.Parameters[0].Value = Serw_run;
                            cmd.Parameters[1].Value = DateTime.Now;
                            cmd.Parameters[2].Value = matchQuery.Count();
                            cmd.Parameters[3].Value = Log_rek;
                            cmd.Parameters[4].Value = System.Guid.NewGuid();
                            cmd.ExecuteNonQuery();
                        }
                        using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                            "DELETE FROM public.server_query " +
                            "WHERE start_date<current_timestamp - interval '14 day '  ", conA))
                        {
                            cmd.ExecuteNonQuery();
                        }
                        tr_savelogs.Commit();
                    }
                }
            }
            catch (Exception e)
            {
                Log("No i dupa " + e);
            }
        }
        private async Task<int> Kick_off_err_orders(int dop_id, DateTime mod_date)
        {
            try
            {
                //  procedura do przesuwania zamówień po wystapieniu błedu w użyciu standardowych instrukcji
                using (OracleConnection conO = new OracleConnection("Password=pass;User ID = user; Data Source = prod8"))
                {
                    string anonymous_block = " DECLARE " +
                                " empty VARCHAR2(32000) := NULL; " +
                                " b_ VARCHAR2(32000); " +
                                " c_ VARCHAR2(32000); " +
                                " de_ DATE := :move_date; " +
                                " d_ VARCHAR2(32000); " +
                                " dop VARCHAR2(32000) := :DP_ID; " +
                                " BEGIN " +
                                " d_:= 'DUE_DATE' || chr(31) || To_Char(de_, 'YYYY-MM-DD-HH24.MI.SS') || chr(30) || 'REPLICATE_CHANGES' || chr(31) || 'TRUE' || chr(30) || 'SEND_CHANGE_MESSAGE' || chr(31) || 'FALSE' || chr(30); " +
                                " SELECT objid into b_ FROM ifsapp.dop_head WHERE dop_id = dop; " +
                                " SELECT objversion into c_ FROM ifsapp.dop_head WHERE dop_id = dop; " +
                                " IFSAPP.DOP_HEAD_API.MODIFY__(empty, b_, c_, d_, 'DO'); " +
                                " IFSAPP.Dop_Order_API.Modify_Top_Level_Date__(empty, dop, 1, de_, 'YES'); " +
                                " IFSAPP.Dop_Demand_Gen_API.Modify_Connected_Order_Line(empty, dop, 'DATE', 'FALSE', 'TRUE'); " +
                                " FOR item IN " +
                                " ( " +
                                " select DOP_ID, DOP_ORDER_ID, IDENTITY1, IDENTITY2, IDENTITY3, SOURCE_DB from IFSAPP.DOP_ALARM where IFSAPP.DOP_ORDER_API.Get_State(dop_id, DOP_ORDER_ID) != 'Rozp.' and DOP_ID = dop and IFSAPP.Dop_Order_API.Get_Objstate(DOP_ID, DOP_ORDER_ID) <> 'Closed' " +
                                ") " +
                                " LOOP " +
                                " IFSAPP.Dop_Supply_Gen_API.Adjust_Pegged_Date(item.IDENTITY1, item.IDENTITY2, item.IDENTITY3, item.SOURCE_DB, item.DOP_ID, item.DOP_ORDER_ID); " +
                                " END LOOP; " +
                                " COMMIT; " +
                                " END; ";
                    await conO.OpenAsync();
                    OracleGlobalization info = conO.GetSessionInfo();
                    info.DateFormat = "YYYY-MM-DD";
                    conO.SetSessionInfo(info);
                    using (OracleCommand comm = conO.CreateCommand())
                    {
                        comm.CommandText = anonymous_block;
                        comm.Parameters.Add(":DP_ID", OracleDbType.Varchar2).Value = dop_id.ToString();
                        comm.Parameters.Add(":move_date", OracleDbType.Varchar2).Value = mod_date;
                        comm.Prepare();
                        comm.ExecuteNonQuery();
                    }

                }
                return 0;
            }
            catch (Exception e)
            {
                Log("Błąd modyfikacji daty produkcji (procedura awaryjna):" + e);
                return 1;
            }
        }
        #region Słownik danych dla elementów typu OLEDB
        static NpgsqlConnectionStringBuilder qw = new NpgsqlConnectionStringBuilder()
        {
            Host = "10.0.1.29",
            Port = 5432,
            CommandTimeout = 0,
            ConnectionIdleLifetime = 50,
            ApplicationName = "ZAKUPY_CONNECTOR",
            Username = "USER",
            Password = "pass",
            Database = "zakupy"
        };
        static NpgsqlConnectionStringBuilder qwa = new NpgsqlConnectionStringBuilder()
        {
            Host = "10.0.1.29",
            Port = 5432,
            ConnectionIdleLifetime = 30,
            ApplicationName = "ZAKUPY_CONNECTOR",
            Username = "USER",
            Password = "pass",
            Database = "zakupy"
        };
        readonly string npA = qw.ToString();
        readonly string npC = qwa.ToString();
        DateTime start = DateTime.Now;
        readonly Dictionary<Type, NpgsqlTypes.NpgsqlDbType> typeMap = new Dictionary<Type, NpgsqlTypes.NpgsqlDbType>
        {
            [typeof(byte)] = NpgsqlTypes.NpgsqlDbType.Bigint,
            [typeof(string)] = NpgsqlTypes.NpgsqlDbType.Varchar,
            [typeof(int)] = NpgsqlTypes.NpgsqlDbType.Integer,
            [typeof(bool)] = NpgsqlTypes.NpgsqlDbType.Boolean,
            [typeof(DateTime)] = NpgsqlTypes.NpgsqlDbType.Date,
            [typeof(char)] = NpgsqlTypes.NpgsqlDbType.Char,
            [typeof(decimal)] = NpgsqlTypes.NpgsqlDbType.Double,
            [typeof(double)] = NpgsqlTypes.NpgsqlDbType.Double,
            [typeof(Single)] = NpgsqlTypes.NpgsqlDbType.Smallint,
            [typeof(Guid)] = NpgsqlTypes.NpgsqlDbType.Uuid
        };

        #endregion
        DataTable StMag = new DataTable(); // dane stanu magazynowego z ORACLE
        DataTable DMND_ORA = new DataTable(); // Potrzeby z ORACLE
        DataTable DEMANDS = new DataTable(); // Dane z tabeli ACCESS (potrzeby) ,dane wspomagające
        DataTable POTW = new DataTable(); // Dane z ACCESS (potwierdzenia braków)
        DataTable dta = new DataTable(); // Dane z ACCESS - formatka dla zakupów
        DataTable TASK_DEMANDS = new DataTable(); //tabela czynności dla DEMANDS
        DataTable TASK_dat = new DataTable();////tabela czynności dla dat
        DataTable TMPTASK_dat = new DataTable();
        DataTable MShedul = new DataTable();
        DataTable eras_Shedul = new DataTable();
        #region Moduł serwera
        public void Run()
        {
            try
            {
                serv_state = "Started";

                #region Definicje publiczne
                // Określ ścieżkę tabeli danych ACCESS
                // Nadaj Nazwę głównej tabeli danych
                MShedul.Columns.Add("PART_NO", System.Type.GetType("System.String"));
                MShedul.Columns.Add("Dat", System.Type.GetType("System.DateTime"));
                MShedul.Columns.Add("Purch_rpt", System.Type.GetType("System.Boolean"));
                MShedul.Columns.Add("Purch_rpt_dat", System.Type.GetType("System.DateTime"));
                MShedul.Columns.Add("Braki_rpt", System.Type.GetType("System.Boolean"));
                MShedul.Columns.Add("Braki_rpt_dat", System.Type.GetType("System.DateTime"));
                MShedul.Columns.Add("Kol", System.Type.GetType("System.Int32"));
                eras_Shedul.Columns.Add("PART_NO", System.Type.GetType("System.String"));
                eras_Shedul.Columns.Add("Dat", System.Type.GetType("System.DateTime"));
                // zbierz wielowątkowo dane
                using (NpgsqlConnection conA = new NpgsqlConnection(npC))
                {
                    conA.Open();
                    using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                        "iNSERT INTO public.datatbles " +
                        "(table_name, last_modify, start_update, in_progress, updt_errors) " +
                        "SELECT a.table_name,current_timestamp,current_timestamp,false,false " +
                          "FROM " +
                                "information_schema.tables a " +
                            "left join " +
                                "datatbles b " +
                            "on b.table_name=a.table_name " +
                           "WHERE " +
                           "a.table_schema='public' and a.table_name not in('type_dmd','datatbles') " +
                           "and table_type='BASE TABLE' and b.table_name is null", conA))
                    {
                        cmd.ExecuteNonQuery();
                    }
                    using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                        "UPDATE public.datatbles	" +
                        "SET start_update=current_timestamp, in_progress=true " +
                        "WHERE table_name='server_progress'", conA))
                    {
                        cmd.ExecuteNonQuery();
                    }
                }
                int main = 0;
                int run_cust = 0;
                int red = 0;
                int red1 = 0;
                int red2 = 0;

                Parallel.Invoke(srv_op, async () => run_cust = await CUST_ord(), async () => main = await RUN_CALC());
                int wy = run_cust + main;
                if (wy == 0)
                {
                    bool chk = true;
                    using (NpgsqlConnection conA = new NpgsqlConnection(npA))
                    {
                        conA.Open();
                        using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                            "select cast(count(table_name) as integer) busy " +
                            "from " +
                            "public.datatbles " +
                            "where " +
                            "(substring(table_name,1,6)='worker' or table_name='demands'  " +
                                "or table_name='wrk_del' or table_name='cust_ord' or table_name='main_loop') " +
                            "and in_progress=true", conA))
                        {
                            int busy_il = 1;
                            while (busy_il > 0)
                            {
                                busy_il = Convert.ToInt16(cmd.ExecuteScalar());
                                if (busy_il > 0) { System.Threading.Thread.Sleep(250); }
                                if (start.AddMinutes(7) < DateTime.Now)
                                {
                                    chk = false;
                                    busy_il = 0;
                                }
                            }
                        }
                    }
                    GC.Collect();
                    if (chk) { Parallel.Invoke(srv_op, async () => red = await Calculate_cust_ord(), async () => red1 = await All_lacks(), async () => red2 = await Lack_bil()); }
                }
                else
                {
                    if (wy == 1)
                    {
                        if (run_cust == 1)
                        {
                            Parallel.Invoke(srv_op, async () => run_cust = await CUST_ord());
                        }
                        if (main == 1)
                        {
                            Parallel.Invoke(srv_op, async () => main = await RUN_CALC());
                        }
                    }
                    else
                    {
                        Parallel.Invoke(srv_op, async () => run_cust = await CUST_ord(), async () => main = await RUN_CALC());
                    }
                    wy = run_cust + main;
                    if (wy == 0)
                    {
                        bool chk = true;
                        using (NpgsqlConnection conA = new NpgsqlConnection(npA))
                        {
                            conA.Open();
                            using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                                "select cast(count(table_name) as integer) busy " +
                                "from public.datatbles " +
                                "where " +
                                "(substring(table_name,1,6)='worker' or table_name='demands' or " +
                                    "table_name='cust_ord' or table_name='main_loop') " +
                                "and in_progress=true", conA))
                            {
                                int busy_il = 1;
                                while (busy_il > 0)
                                {
                                    busy_il = Convert.ToInt16(cmd.ExecuteScalar());
                                    if (busy_il > 0) { System.Threading.Thread.Sleep(250); }
                                    if (start.AddMinutes(7) < DateTime.Now)
                                    {
                                        chk = false;
                                        busy_il = 0;
                                    }
                                }
                            }
                        }
                        GC.Collect();
                        if (chk) { Parallel.Invoke(srv_op, async () => red = await Calculate_cust_ord(), async () => red1 = await All_lacks(), async () => red2 = await Lack_bil()); }
                    }
                }
                if (run_cust == 1) { Log("Błąd pobrania danych dotyczących zamówień z ORACLE"); }
                if (main == 1) { Log("Błąd obliczeń w pętli głównej programu"); }
                using (NpgsqlConnection conA = new NpgsqlConnection(npC))
                {
                    conA.Open();
                    using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                        "UPDATE public.datatbles " +
                        "SET last_modify=current_timestamp, in_progress=false,updt_errors=false " +
                        "WHERE table_name='server_progress'", conA))
                    {
                        cmd.ExecuteNonQuery();
                    }
                }

                GC.Collect();
                serv_state = "Ready";

                #endregion
                #region Główne obliczenia
                async Task<int> RUN_CALC()
                {
                    try
                    {
                        // Sprawdź czy główna baza danych istnieje - jeśli nie to utwórz bazę (Biblioteka ADOX)
                        #region UTwórz Bazę danych
                        // Nie AKTUALNE :)

                        Log("CREATE END " + (DateTime.Now - start));
                        #endregion
                        using (NpgsqlConnection conA = new NpgsqlConnection(npC))
                        {
                            await conA.OpenAsync();
                            using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                                "UPDATE public.datatbles " +
                                "SET start_update=current_timestamp, in_progress=true " +
                                "WHERE substring(table_name,1,6)='worker'", conA))
                            {
                                cmd.ExecuteNonQuery();
                            }
                            conA.Close();
                        }
                        //Uruchom Równolegle i Asynchroniczne zadania pobrania danych z tabeli ORACLE
                        #region Asynchroniczne zadania wykonywane w tym samym momencie
                        int eNd_DM = 0;
                        int eND_STm = 0;
                        int eND_kal = 0;
                        Parallel.Invoke(srv_op, async () => eNd_DM = await Get_DMNDO(), async () => eND_STm = await Get_STmagO(), async () => eND_kal = await Get_Kalendar());

                        if (eND_kal == 1) { Log("Błąd pobrania danych dotyczących kalendarza firmy z ORACLE"); }
                        if (eNd_DM == 1) { Log("Błąd pobrania danych dotyczących potrzeb materiałowych z ORACLE"); }
                        if (eND_STm == 1) { Log("Błąd pobrania danych dotyczących stanu magazynowego z ORACLE"); }
                        #endregion
                        GC.Collect();
                        Serw_run = start;
                        Log("Start work " + (DateTime.Now - start));
                        if (eNd_DM + eND_kal + eND_STm == 0)
                        {
                            // Poczekaj na zadania i Uruchom Asynchroniczne pobranie danych z tabel ACCESS

                            #region Definicje
                            // No to do roboty !!!
                            DateTime nullDAT = start.AddDays(1000);
                            DateTime range_Dat = nullDAT;
                            using (NpgsqlConnection conB = new NpgsqlConnection(npC))
                            {
                                await conB.OpenAsync();
                                {
                                    using (NpgsqlCommand cmd = new NpgsqlCommand("select date_fromnow(10);", conB))
                                    {
                                        range_Dat = (DateTime)cmd.ExecuteScalar();
                                    }
                                }
                                conB.Close();
                            }
                            string Part_no = "";
                            int ind_mag = 0;
                            int ind_Demands = 0;
                            int ind_DTA = 0;
                            DateTime Date_reQ;
                            byte TYP_dmd = 0;
                            byte b_DOP = byte.Parse("2");
                            byte b_ZAM = byte.Parse("4");
                            byte b_zlec = byte.Parse("1");
                            string KOOR = "";
                            string TYPE = "";
                            double SUM_QTY_SUPPLY = 0;
                            double Sum_dost_op = 0;
                            double Sum_potrz_op = 0;
                            double SUM_QTY_DEMAND = 0;
                            double QTY_SUPPLY = 0;
                            double QTY_DEMAND = 0;
                            double DEMAND_ZAM = 0;
                            double QTY_DEMAND_DOP = 0;
                            double STAN_mag = 0;
                            double Chk_Sum = 0;
                            double bilans = 0;
                            double balance = 0;
                            double Balane_mag = 0;
                            double dmnNext = 0;
                            int chksumD = 0;
                            Int16 leadtime = 0;
                            DateTime widoczny = nullDAT;
                            int counter = -1;
                            int max = DMND_ORA.Rows.Count;
                            int MAX_DEMANDs = DEMANDS.Rows.Count;
                            int MAX_Dta = dta.Rows.Count;
                            DateTime gwar_DT = nullDAT;
                            DateTime Data_Braku = nullDAT;
                            int par = Convert.ToInt32(DateTime.Now.Date.ToOADate());
                            if (par % 2 == 0) { par = 1; } else { par = 0; }
                            DateTime DATNOW = DateTime.Now.Date;
                            DateTime dat_CREA = nullDAT;
                            DataRow NEXT_row = DMND_ORA.DefaultView[0].Row;
                            Boolean TMP_ARR = false;
                            DateTime rpt_short = nullDAT;
                            DateTime dta_rap = nullDAT;
                            DataTable Shedul = new DataTable();
                            Shedul.Columns.Add("PART_NO", System.Type.GetType("System.String"));
                            Shedul.Columns.Add("Dat", System.Type.GetType("System.DateTime"));
                            Shedul.Columns.Add("in_DB", System.Type.GetType("System.Boolean"));
                            bool in_DB = false;
                            int kol = 0;
                            #endregion
                            using (NpgsqlConnection conA = new NpgsqlConnection(npC))
                            {
                                await conA.OpenAsync();
                                using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                                    "UPDATE public.datatbles " +
                                    "SET start_update=current_timestamp, in_progress=true " +
                                    "WHERE table_name='main_loop'", conA))
                                {
                                    cmd.ExecuteNonQuery();
                                }
                                conA.Close();
                            }
                            foreach (DataRowView rek in DMND_ORA.DefaultView)
                            {
                                if (counter < max) { counter++; }
                                // Zmiana obliczanego indeksu
                                #region USTAW ZMIENNE Skasuj nie występujące indeksy z tabel głównych
                                if (Part_no != rek["Part_no"].ToString())
                                {
                                    try
                                    {
                                        //type_DMD - maska bitowa 0001 - zlec ;0010 - DOP ;0100-zam-klient
                                        TYP_dmd = 0;
                                        Data_Braku = nullDAT;
                                        SUM_QTY_SUPPLY = 0;
                                        SUM_QTY_DEMAND = 0;
                                        Sum_dost_op = 0;
                                        Chk_Sum = 0;
                                        balance = 0;
                                        Balane_mag = 0;
                                        bilans = 0;
                                        dmnNext = 0;
                                        Sum_dost_op = 0;
                                        Sum_potrz_op = 0;
                                        TMP_ARR = false;
                                        if (Part_no != "")
                                        {
                                            if ((rpt_short != nullDAT || dta_rap != nullDAT) && KOOR != "LUCPRZ" && Part_no.Substring(1, 4) != "5700" && Part_no.Substring(1, 4) != "5800")
                                            {
                                                bool AM = false;
                                                foreach (DataRowView row in Shedul.DefaultView)
                                                {
                                                    if (((DateTime)row["Dat"] <= rpt_short && rpt_short != nullDAT && (bool)row["in_DB"] != true) || (((DateTime)row["Dat"] >= dta_rap || DATNOW == dta_rap) && dta_rap != nullDAT) && (DateTime)row["Dat"] < range_Dat && (bool)row["in_DB"] != true)
                                                    {
                                                        AM = true;
                                                        DataRow rw = MShedul.NewRow();
                                                        rw["part_no"] = row["part_no"];
                                                        rw["Dat"] = row["Dat"];
                                                        if (rpt_short != nullDAT)
                                                        {
                                                            rw["Purch_rpt"] = true;
                                                            rw["Purch_rpt_dat"] = rpt_short;
                                                        }
                                                        else
                                                        {
                                                            rw["Purch_rpt"] = false;


                                                        }
                                                        if (dta_rap != nullDAT)
                                                        {
                                                            rw["Braki_rpt"] = true;
                                                            rw["Braki_rpt_dat"] = dta_rap;
                                                        }
                                                        else
                                                        {
                                                            rw["Braki_rpt"] = false;
                                                        }
                                                        rw["kol"] = kol;
                                                        MShedul.Rows.Add(rw);
                                                    }
                                                    else if ((((DateTime)row["Dat"] > rpt_short && rpt_short != nullDAT && (DateTime)row["Dat"] < dta_rap && dta_rap != nullDAT) || ((DateTime)row["Dat"] < dta_rap && dta_rap != nullDAT && rpt_short == nullDAT) || ((DateTime)row["Dat"] > rpt_short && rpt_short != nullDAT && dta_rap == nullDAT)) && (bool)row["in_DB"] == true)
                                                    {
                                                        DataRow rw = eras_Shedul.NewRow();
                                                        rw["part_no"] = Part_no;
                                                        rw["dat"] = row["Dat"];
                                                        eras_Shedul.Rows.Add(rw);
                                                    }
                                                }
                                                if (AM)
                                                {
                                                    kol++;
                                                    if (kol == 5) { kol = 0; }
                                                }
                                            }
                                            else
                                            {
                                                if (in_DB)
                                                {
                                                    DataRow row = eras_Shedul.NewRow();
                                                    row["part_no"] = Part_no;
                                                    eras_Shedul.Rows.Add(row);
                                                }
                                            }
                                        }
                                        Part_no = (string)rek["Part_no"];
                                        Shedul.Clear();
                                        rpt_short = nullDAT;
                                        dta_rap = nullDAT;
                                        in_DB = false;
                                        while (Part_no != StMag.DefaultView[ind_mag].Row["Indeks"].ToString())
                                        {
                                            string str_mag = StMag.DefaultView[ind_mag].Row["Indeks"].ToString();
                                            if (MAX_DEMANDs > ind_Demands)
                                            {
                                                int tmp_count = 0;
                                                while (str_mag == DEMANDS.DefaultView[ind_Demands].Row["Part_NO"].ToString())
                                                {
                                                    if (tmp_count == 0)
                                                    {
                                                        DataRow row = eras_Shedul.NewRow();
                                                        row["part_no"] = Part_no;
                                                        eras_Shedul.Rows.Add(row);

                                                        DataRow rw = TASK_DEMANDS.NewRow();
                                                        rw["PART_NO"] = str_mag;
                                                        rw["Czynnosc"] = "DEL_PART_DEM";
                                                        rw["Status"] = "Zapl.";
                                                        TASK_DEMANDS.Rows.Add(rw);
                                                    }
                                                    tmp_count++;
                                                    ind_Demands++;
                                                    if (MAX_DEMANDs <= ind_Demands) { break; }
                                                }
                                            }

                                            if (MAX_Dta > ind_DTA)
                                            {
                                                int tmp_count = 0;
                                                while (str_mag == dta.DefaultView[ind_DTA].Row["Indeks"].ToString())
                                                {
                                                    if (tmp_count == 0)
                                                    {
                                                        DataRow rw = TASK_dat.NewRow();
                                                        rw["indeks"] = str_mag;
                                                        rw["Czynnosc"] = "DEL_PART_DEM";
                                                        rw["Status"] = "Zapl.";
                                                        TASK_dat.Rows.Add(rw);
                                                    }
                                                    tmp_count++;
                                                    ind_DTA++;
                                                    if (MAX_Dta <= ind_DTA) { break; }
                                                }

                                            }
                                            ind_mag++;
                                        }
                                        STAN_mag = Convert.ToDouble(StMag.DefaultView[ind_mag].Row["MAG"]);
                                        gwar_DT = (DateTime)StMag.DefaultView[ind_mag].Row["Data_gwarancji"];
                                        leadtime = Convert.ToInt16(StMag.DefaultView[ind_mag].Row["Czas_dostawy"]);
                                        KOOR = (string)StMag.DefaultView[ind_mag].Row["PLANNER_BUYER"];
                                        TYPE = (string)StMag.DefaultView[ind_mag].Row["RODZAJ"] ?? "NULL";
                                    }
                                    catch (Exception e)
                                    {
                                        using (NpgsqlConnection conA = new NpgsqlConnection(npC))
                                        {
                                            await conA.OpenAsync();
                                            using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                                                "UPDATE public.datatbles " +
                                                "SET in_progress=false,updt_errors=true " +
                                                "WHERE table_name='main_loop'", conA))
                                            {
                                                cmd.ExecuteNonQuery();
                                            }
                                            conA.Close();
                                        }
                                        Log("Błąd przypisania danych magazynowych: " + e);
                                        return 1;
                                    }
                                }
                                #endregion
                                // Dane pobierane dla wiersza
                                #region Pobierz z bieżącego rekordu i oblicz dane na dzień / indeks
                                try
                                {
                                    Date_reQ = (DateTime)rek["DATE_REQUIRED"];
                                    QTY_SUPPLY = Convert.ToDouble(rek["QTY_SUPPLY"]);
                                    QTY_DEMAND = Convert.ToDouble(rek["QTY_DEMAND"]);
                                    DEMAND_ZAM = Convert.ToDouble(rek["DEMAND_ZAM"]);
                                    QTY_DEMAND_DOP = Convert.ToDouble(rek["QTY_DEMAND_DOP"]);
                                    chksumD = Convert.ToInt32(rek["chk_sum"]);
                                    // Sprawdź pochodzenie potrzeby 
                                    if (QTY_DEMAND_DOP > 0) { TYP_dmd = (byte)(TYP_dmd | b_DOP); }
                                    if (DEMAND_ZAM > 0) { TYP_dmd = (byte)(TYP_dmd | b_ZAM); }
                                    if (QTY_DEMAND != QTY_DEMAND_DOP + DEMAND_ZAM) { TYP_dmd = (byte)(TYP_dmd | b_zlec); }
                                    balance = STAN_mag + SUM_QTY_SUPPLY - SUM_QTY_DEMAND;
                                    SUM_QTY_SUPPLY = SUM_QTY_SUPPLY + QTY_SUPPLY;
                                    SUM_QTY_DEMAND = SUM_QTY_DEMAND + QTY_DEMAND;
                                    Balane_mag = STAN_mag - SUM_QTY_DEMAND;
                                    if (dta_rap == nullDAT)
                                    {
                                        if (Balane_mag < 0 && Date_reQ < range_Dat)
                                        {
                                            dta_rap = Date_reQ;
                                        }
                                    }
                                    if ((STAN_mag + SUM_QTY_SUPPLY - SUM_QTY_DEMAND < 0 || (Balane_mag < 0 && Date_reQ <= DATNOW)) && Data_Braku == nullDAT)
                                    {
                                        Data_Braku = Date_reQ;
                                        if (MAX_DEMANDs > ind_Demands && DEMANDS.DefaultView[ind_Demands].Row["DAT_SHORTAGE"] != DBNull.Value)
                                        {
                                            widoczny = Convert.ToDateTime(MAX_DEMANDs > ind_Demands ? DEMANDS.DefaultView[ind_Demands].Row["DAT_SHORTAGE"] ?? start : start);
                                        }
                                        else
                                        {
                                            widoczny = start;
                                        }
                                    }
                                    Chk_Sum = (leadtime + SUM_QTY_SUPPLY + SUM_QTY_DEMAND) * TYP_dmd - (QTY_SUPPLY + QTY_DEMAND) + par;
                                    dmnNext = 0;
                                    if (counter < max - 1)
                                    {
                                        NEXT_row = DMND_ORA.DefaultView[counter + 1].Row;
                                    }
                                    bilans = STAN_mag + SUM_QTY_SUPPLY - SUM_QTY_DEMAND - QTY_SUPPLY;
                                }
                                catch (Exception e)
                                {
                                    using (NpgsqlConnection conA = new NpgsqlConnection(npC))
                                    {
                                        await conA.OpenAsync();
                                        using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                                            "UPDATE public.datatbles " +
                                            "SET in_progress=false,updt_errors=true " +
                                            "WHERE table_name='main_loop'", conA))
                                        {
                                            cmd.ExecuteNonQuery();
                                        }
                                        conA.Close();
                                    }
                                    Log("Błąd przypisania zmiennych rekordu: " + e);
                                    return 1;
                                }
                                #endregion
                                #region Oblicz Dane do formatki Demands
                                try
                                {
                                    if (MAX_DEMANDs > ind_Demands)
                                    {
                                        if (Part_no == DEMANDS.DefaultView[ind_Demands].Row["Part_No"].ToString())
                                        {
                                            while ((DateTime)DEMANDS.DefaultView[ind_Demands].Row["WORK_DAY"] < Date_reQ && Part_no == DEMANDS.DefaultView[ind_Demands].Row["Part_No"].ToString())
                                            {
                                                DataRow row = eras_Shedul.NewRow();
                                                row["part_no"] = Part_no;
                                                row["Dat"] = Date_reQ;
                                                eras_Shedul.Rows.Add(row);

                                                DataRow rw = TASK_DEMANDS.NewRow();
                                                rw["PART_NO"] = Part_no;
                                                rw["WORK_DAY"] = Date_reQ;
                                                rw["Src_id"] = DEMANDS.DefaultView[ind_Demands].Row["ID"];
                                                rw["Czynnosc"] = "DEL_ID_DEM";
                                                rw["Status"] = "Zapl.";
                                                TASK_DEMANDS.Rows.Add(rw);
                                                ind_Demands++;
                                                if (MAX_DEMANDs <= ind_Demands) { break; }
                                            }
                                            if (MAX_DEMANDs > ind_Demands)
                                            {
                                                if ((DateTime)DEMANDS.DefaultView[ind_Demands].Row["WORK_DAY"] == Date_reQ && Part_no == DEMANDS.DefaultView[ind_Demands].Row["Part_No"].ToString())
                                                {
                                                    bool war = false;
                                                    if (DEMANDS.DefaultView[ind_Demands].Row["inDB"] != DBNull.Value)
                                                    {
                                                        if ((bool)DEMANDS.DefaultView[ind_Demands].Row["inDB"] == true)
                                                        {
                                                            in_DB = true;
                                                        }
                                                    }
                                                    if (chksumD != Convert.ToInt32(DEMANDS.DefaultView[ind_Demands].Row["CHKSUM"]) || TYP_dmd != Convert.ToByte(DEMANDS.DefaultView[ind_Demands].Row["TYPE_DMD"]) || Chk_Sum != Convert.ToDouble(DEMANDS.DefaultView[ind_Demands].Row["chk_sum"]) || QTY_SUPPLY != Convert.ToDouble(DEMANDS.DefaultView[ind_Demands].Row["purch_qty"]) || QTY_DEMAND != Convert.ToDouble(DEMANDS.DefaultView[ind_Demands].Row["qty_demand"]))
                                                    {
                                                        DataRow row = Shedul.NewRow();
                                                        row["part_no"] = Part_no;
                                                        row["Dat"] = Date_reQ;
                                                        row["in_DB"] = false;
                                                        Shedul.Rows.Add(row);
                                                        war = true;
                                                    }
                                                    else
                                                    {
                                                        DataRow row = Shedul.NewRow();
                                                        row["part_no"] = Part_no;
                                                        row["Dat"] = Date_reQ;
                                                        bool ch = false;
                                                        if (DEMANDS.DefaultView[ind_Demands].Row["inDB"] == DBNull.Value)
                                                        {
                                                            ch = false;
                                                        }
                                                        else
                                                        {
                                                            ch = (bool)DEMANDS.DefaultView[ind_Demands].Row["inDB"];
                                                        }
                                                        row["in_DB"] = ch;
                                                        Shedul.Rows.Add(row);
                                                    }
                                                    if (war || leadtime != Convert.ToInt32(DEMANDS.DefaultView[ind_Demands].Row["expected_leadtime"]) || (STAN_mag + SUM_QTY_SUPPLY - SUM_QTY_DEMAND) != Convert.ToDouble(DEMANDS.DefaultView[ind_Demands].Row["balance"]) || (STAN_mag - SUM_QTY_DEMAND) != Convert.ToDouble(DEMANDS.DefaultView[ind_Demands].Row["bal_stock"]))
                                                    {
                                                        DataRow rw = TASK_DEMANDS.NewRow();
                                                        rw["PART_NO"] = Part_no;
                                                        rw["WORK_DAY"] = Date_reQ;
                                                        rw["PURCH_QTY"] = QTY_SUPPLY;
                                                        rw["QTY_DEMAND"] = QTY_DEMAND;
                                                        rw["EXPECTED_LEADTIME"] = leadtime;
                                                        rw["TYPE_DMD"] = TYP_dmd;
                                                        rw["BALANCE"] = STAN_mag + SUM_QTY_SUPPLY - SUM_QTY_DEMAND;
                                                        rw["BAL_STOCK"] = STAN_mag - SUM_QTY_DEMAND;
                                                        rw["KOOR"] = KOOR;
                                                        rw["TYPE"] = TYPE;
                                                        if ((STAN_mag + SUM_QTY_SUPPLY - SUM_QTY_DEMAND < 0 || (Balane_mag < 0 && Date_reQ <= start)) && DEMANDS.DefaultView[ind_Demands].Row["DAT_SHORTAGE"].ToString() == "")
                                                        {
                                                            rw["DAT_SHORTAGE"] = start;
                                                        }
                                                        else
                                                        {
                                                            rw["DAT_SHORTAGE"] = DEMANDS.DefaultView[ind_Demands].Row["DAT_SHORTAGE"];
                                                        }
                                                        rw["Src_id"] = DEMANDS.DefaultView[ind_Demands].Row["ID"];
                                                        rw["Czynnosc"] = "MoD_DEM";
                                                        rw["Status"] = "Zapl.";
                                                        rw["CHK_SUM"] = Chk_Sum;
                                                        rw["objversion"] = start;
                                                        rw["chksum"] = chksumD;
                                                        TASK_DEMANDS.Rows.Add(rw);
                                                    }
                                                    ind_Demands++;
                                                }
                                                else
                                                {
                                                    DataRow row = Shedul.NewRow();
                                                    row["part_no"] = Part_no;
                                                    row["Dat"] = Date_reQ;
                                                    row["in_DB"] = false;
                                                    Shedul.Rows.Add(row);

                                                    DataRow rw = TASK_DEMANDS.NewRow();
                                                    rw["PART_NO"] = Part_no;
                                                    rw["WORK_DAY"] = Date_reQ;
                                                    rw["EXPECTED_LEADTIME"] = leadtime;
                                                    rw["PURCH_QTY"] = QTY_SUPPLY;
                                                    rw["QTY_DEMAND"] = QTY_DEMAND;
                                                    rw["TYPE_DMD"] = TYP_dmd;
                                                    rw["BALANCE"] = balance;
                                                    rw["BAL_STOCK"] = Balane_mag;
                                                    rw["KOOR"] = KOOR;
                                                    rw["TYPE"] = TYPE;
                                                    if (balance < 0 || (Balane_mag < 0 && Date_reQ <= start))
                                                    {
                                                        rw["DAT_SHORTAGE"] = start;
                                                    }
                                                    rw["Czynnosc"] = "ADD_NEW_DEM";
                                                    rw["Status"] = "Zapl.";
                                                    rw["CHK_SUM"] = Chk_Sum;
                                                    rw["objversion"] = start;
                                                    rw["chksum"] = chksumD;
                                                    TASK_DEMANDS.Rows.Add(rw);
                                                }
                                            }
                                            else
                                            {
                                                DataRow row = Shedul.NewRow();
                                                row["part_no"] = Part_no;
                                                row["Dat"] = Date_reQ;
                                                row["in_DB"] = false;
                                                Shedul.Rows.Add(row);

                                                DataRow rw = TASK_DEMANDS.NewRow();
                                                rw["PART_NO"] = Part_no;
                                                rw["WORK_DAY"] = Date_reQ;
                                                rw["EXPECTED_LEADTIME"] = leadtime;
                                                rw["PURCH_QTY"] = QTY_SUPPLY;
                                                rw["QTY_DEMAND"] = QTY_DEMAND;
                                                rw["TYPE_DMD"] = TYP_dmd;
                                                rw["BALANCE"] = balance;
                                                rw["BAL_STOCK"] = Balane_mag;
                                                rw["KOOR"] = KOOR;
                                                rw["TYPE"] = TYPE;
                                                if (balance < 0 || (Balane_mag < 0 && Date_reQ <= start))
                                                {
                                                    rw["DAT_SHORTAGE"] = start;
                                                }
                                                rw["Czynnosc"] = "ADD_NEW_DEM";
                                                rw["Status"] = "Zapl.";
                                                rw["CHK_SUM"] = Chk_Sum;
                                                rw["objversion"] = start;
                                                rw["chksum"] = chksumD;
                                                TASK_DEMANDS.Rows.Add(rw);
                                            }
                                        }
                                        else
                                        {
                                            DataRow row = Shedul.NewRow();
                                            row["part_no"] = Part_no;
                                            row["Dat"] = Date_reQ;
                                            row["in_DB"] = false;
                                            Shedul.Rows.Add(row);

                                            DataRow rw = TASK_DEMANDS.NewRow();
                                            rw["PART_NO"] = Part_no;
                                            rw["WORK_DAY"] = Date_reQ;
                                            rw["EXPECTED_LEADTIME"] = leadtime;
                                            rw["PURCH_QTY"] = QTY_SUPPLY;
                                            rw["QTY_DEMAND"] = QTY_DEMAND;
                                            rw["TYPE_DMD"] = TYP_dmd;
                                            rw["BALANCE"] = balance;
                                            rw["BAL_STOCK"] = Balane_mag;
                                            rw["KOOR"] = KOOR;
                                            rw["TYPE"] = TYPE;
                                            if (balance < 0 || (Balane_mag < 0 && Date_reQ <= start))
                                            {
                                                rw["DAT_SHORTAGE"] = start;
                                            }
                                            rw["Czynnosc"] = "ADD_NEW_DEM";
                                            rw["Status"] = "Zapl.";
                                            rw["CHK_SUM"] = Chk_Sum;
                                            rw["objversion"] = start;
                                            rw["chksum"] = chksumD;
                                            TASK_DEMANDS.Rows.Add(rw);
                                        }
                                        // Sprawdź czy następny rekord będzie miał inny indeks
                                        if (counter < max - 1 && MAX_DEMANDs > ind_Demands)
                                        {
                                            if (Part_no != NEXT_row["part_no"].ToString())
                                            {
                                                while ((DateTime)DEMANDS.DefaultView[ind_Demands].Row["WORK_DAY"] > Date_reQ && Part_no == DEMANDS.DefaultView[ind_Demands].Row["Part_No"].ToString())
                                                {
                                                    DataRow row = eras_Shedul.NewRow();
                                                    row["part_no"] = Part_no;
                                                    row["Dat"] = Date_reQ;
                                                    eras_Shedul.Rows.Add(row);

                                                    DataRow rw = TASK_DEMANDS.NewRow();
                                                    rw["PART_NO"] = Part_no;
                                                    rw["WORK_DAY"] = Date_reQ;
                                                    rw["Src_id"] = DEMANDS.DefaultView[ind_Demands].Row["ID"];
                                                    rw["Czynnosc"] = "DEL_ID_DEM";
                                                    rw["Status"] = "Zapl.";
                                                    TASK_DEMANDS.Rows.Add(rw);
                                                    ind_Demands++;
                                                    if (MAX_DEMANDs <= ind_Demands) { break; }
                                                }
                                            }
                                        }
                                    }
                                    else
                                    {
                                        DataRow row = Shedul.NewRow();
                                        row["part_no"] = Part_no;
                                        row["Dat"] = Date_reQ;
                                        row["in_DB"] = false;
                                        Shedul.Rows.Add(row);

                                        DataRow rw = TASK_DEMANDS.NewRow();
                                        rw["PART_NO"] = Part_no;
                                        rw["WORK_DAY"] = Date_reQ;
                                        rw["EXPECTED_LEADTIME"] = leadtime;
                                        rw["PURCH_QTY"] = QTY_SUPPLY;
                                        rw["QTY_DEMAND"] = QTY_DEMAND;
                                        rw["TYPE_DMD"] = TYP_dmd;
                                        rw["BALANCE"] = balance;
                                        rw["BAL_STOCK"] = Balane_mag;
                                        rw["KOOR"] = KOOR;
                                        rw["TYPE"] = TYPE;
                                        if (balance < 0 || (Balane_mag < 0 && Date_reQ <= start))
                                        {
                                            rw["DAT_SHORTAGE"] = start;
                                        }
                                        rw["Czynnosc"] = "ADD_NEW_DEM";
                                        rw["Status"] = "Zapl.";
                                        rw["CHK_SUM"] = Chk_Sum;
                                        rw["objversion"] = start;
                                        rw["chksum"] = chksumD;
                                        TASK_DEMANDS.Rows.Add(rw);
                                    }
                                }
                                catch (Exception e)
                                {
                                    using (NpgsqlConnection conA = new NpgsqlConnection(npC))
                                    {
                                        await conA.OpenAsync();
                                        using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                                            "UPDATE public.datatbles " +
                                            "SET in_progress=false,updt_errors=true " +
                                            "WHERE table_name='main_loop'", conA))
                                        {
                                            cmd.ExecuteNonQuery();
                                        }
                                        conA.Close();
                                    }
                                    Log("Błąd obliczania formatki DEMANDS :" + e);
                                    return 1;
                                }
                                #endregion
                                #region Oblicz dane do formatki Zakupy
                                try
                                {
                                    // sprawdź potrzeby przed bieżącą datą w przypadku dostawy lub braku zamówień utwórz tabelę tymczasową z rekordami i skumuluj je później
                                    #region Opóźnione dostawy i bieżące dostawy
                                    if (Date_reQ <= DATNOW)
                                    {
                                        Sum_dost_op = Sum_dost_op + QTY_SUPPLY;
                                        Sum_potrz_op = Sum_potrz_op + QTY_DEMAND;
                                        if (STAN_mag - Sum_potrz_op < 0 && Date_reQ == DATNOW)
                                        {
                                            rpt_short = Date_reQ;
                                        }

                                        if (QTY_SUPPLY > 0)
                                        {
                                            bool tstchk = false;

                                            string state = Date_reQ == DATNOW ? "Dzisiejsza dostawa" : "Opóźniona dostawa";
                                            if (MAX_Dta > ind_DTA)
                                            {
                                                // skasuj rekordy nie aktualne rekordy w dta (indeks ten sam tylko daty wcześniejsze)
                                                while ((DateTime)dta.DefaultView[ind_DTA].Row["DATA_DOST"] < Date_reQ && Part_no == dta.DefaultView[ind_DTA].Row["indeks"].ToString())
                                                {
                                                    DataRow row = TASK_dat.NewRow();
                                                    row["indeks"] = Part_no;
                                                    row["DATA_DOST"] = Date_reQ;
                                                    row["Src_id"] = dta.DefaultView[ind_DTA].Row["ID"];
                                                    row["Czynnosc"] = "DEL_ID_DEM";
                                                    row["Status"] = "Zapl.";
                                                    TASK_dat.Rows.Add(row);
                                                    ind_DTA++;
                                                    if (MAX_Dta <= ind_DTA) { break; }
                                                }
                                                if (MAX_Dta > ind_DTA)
                                                {
                                                    if ((DateTime)dta.DefaultView[ind_DTA].Row["DATA_DOST"] == Date_reQ && Part_no == dta.DefaultView[ind_DTA].Row["indeks"].ToString())
                                                    {
                                                        tstchk = true;
                                                    }
                                                }
                                            }
                                            // dostawa opóźniona skopiuj do tymczasowego wiersza w celu podsumowania (bez określenia typu)                                
                                            {
                                                DataRow rw = TMPTASK_dat.NewRow();
                                                rw["indeks"] = Part_no;
                                                rw["OPIS"] = (string)StMag.DefaultView[ind_mag].Row["OPIS"];
                                                rw["KOLEKCJA"] = (string)StMag.DefaultView[ind_mag].Row["KOLEKCJA"];
                                                rw["MAG"] = STAN_mag;
                                                rw["PLANNER_BUYER"] = KOOR;
                                                rw["RODZAJ"] = (string)StMag.DefaultView[ind_mag].Row["RODZAJ"];
                                                rw["CZAS_DOSTAWY"] = (decimal)StMag.DefaultView[ind_mag].Row["CZAS_DOSTAWY"];
                                                rw["DATA_GWARANCJI"] = gwar_DT;
                                                rw["PRZYCZYNA"] = TYP_dmd;
                                                rw["DATA_DOST"] = Date_reQ;
                                                rw["WLK_DOST"] = QTY_SUPPLY;
                                                rw["typ_zdarzenia"] = state;
                                                rw["widoczny_od_dnia"] = MAX_DEMANDs > ind_Demands ? DEMANDS.DefaultView[ind_Demands].Row["DAT_SHORTAGE"] ?? widoczny : start;
                                                rw["Status_informacji"] = "NOT IMPLEMENT";
                                                rw["refr_date"] = start;
                                                if (MAX_Dta > ind_DTA)
                                                {
                                                    if (tstchk)
                                                    {
                                                        rw["Czynnosc"] = "MoD_DEM";
                                                        rw["Status"] = "Zapl.";
                                                        rw["Src_id"] = dta.DefaultView[ind_DTA].Row["id"];
                                                        ind_DTA++;
                                                        if (MAX_Dta > ind_DTA)
                                                        {
                                                            while ((DateTime)dta.DefaultView[ind_DTA].Row["DATA_DOST"] == Date_reQ && Part_no == dta.DefaultView[ind_DTA].Row["indeks"].ToString())
                                                            {
                                                                DataRow row = TASK_dat.NewRow();
                                                                row["indeks"] = Part_no;
                                                                row["DATA_DOST"] = Date_reQ;
                                                                row["Src_id"] = dta.DefaultView[ind_DTA].Row["ID"];
                                                                row["Czynnosc"] = "DEL_ID_DEM";
                                                                row["Status"] = "Zapl.";
                                                                TASK_dat.Rows.Add(row);
                                                                ind_DTA++;
                                                                if (MAX_Dta <= ind_DTA) { break; }
                                                            }
                                                        }
                                                    }
                                                    else
                                                    {
                                                        rw["Czynnosc"] = "ADD_NEW_DEM";
                                                        rw["Status"] = "Zapl.";
                                                    }
                                                }
                                                else
                                                {
                                                    rw["Czynnosc"] = "ADD_NEW_DEM";
                                                    rw["Status"] = "Zapl.";
                                                }
                                                rw["chk"] = Chk_Sum;
                                                TMPTASK_dat.Rows.Add(rw);
                                                TMP_ARR = true;
                                            }
                                        }
                                    }
                                    #endregion
                                    if (TMP_ARR)
                                    {
                                        // zapisz dane z tabeli tymczasowej dla opóźnień do docelowej tabeli zmian
                                        #region Skopiuj Opóżnienia i Brakujące ilości do zamówień i dostawy w dniu prod 
                                        if (Date_reQ > DATNOW || Part_no != NEXT_row["Part_no"].ToString())
                                        {
                                            if (TMPTASK_dat.Rows.Count > 0)
                                            {
                                                if (dta_rap == nullDAT)
                                                {
                                                    if (STAN_mag - Sum_potrz_op < 0)
                                                    {
                                                        dta_rap = Date_reQ;
                                                    }
                                                }
                                                foreach (DataRowView rw in TMPTASK_dat.DefaultView)
                                                {
                                                    DataRow row = TASK_dat.NewRow();
                                                    for (int i = 0; i < TASK_dat.Columns.Count; i++)
                                                    {
                                                        row[i] = rw[i];
                                                    }
                                                    row["bilans"] = STAN_mag - Sum_potrz_op;
                                                    row["bil_dost_dzień"] = STAN_mag - Sum_potrz_op - dmnNext;
                                                    if (Data_Braku != nullDAT)
                                                    {
                                                        row["DATA_BRAKU"] = Data_Braku;
                                                    }
                                                    else
                                                    {
                                                        row["DATA_BRAKU"] = Date_reQ;
                                                    }
                                                    row["sum_dost"] = Sum_dost_op;
                                                    row["sum_potrz"] = Sum_potrz_op;
                                                    row["sum_dost_opóźnion"] = Sum_dost_op;
                                                    row["sum_potrz_opóźnion"] = Sum_potrz_op;
                                                    TASK_dat.Rows.Add(row);
                                                    rw.Delete();
                                                }
                                                TMPTASK_dat.AcceptChanges();
                                                TMP_ARR = false;
                                            }
                                        }
                                        #endregion
                                    }
                                    #region Przyszłe dostawy
                                    if (QTY_SUPPLY > 0)
                                    {
                                        if (Date_reQ > DATNOW && (bilans < 0 || balance < 0))
                                        {
                                            bool tstchk = false;
                                            bool nedupdt = true;
                                            string state = balance < 0 ? "Brakujące ilości" : "Dostawa na dzisiejsze ilości";
                                            if (MAX_Dta > ind_DTA)
                                            {
                                                if (Part_no == dta.DefaultView[ind_DTA].Row["indeks"].ToString())
                                                {
                                                    while ((DateTime)dta.DefaultView[ind_DTA].Row["DATA_DOST"] < Date_reQ && Part_no == dta.DefaultView[ind_DTA].Row["indeks"].ToString())
                                                    {
                                                        DataRow rw = TASK_dat.NewRow();
                                                        rw["indeks"] = Part_no;
                                                        rw["DATA_DOST"] = Date_reQ;
                                                        rw["Src_id"] = dta.DefaultView[ind_DTA].Row["ID"];
                                                        rw["Czynnosc"] = "DEL_ID_DEM";
                                                        rw["Status"] = "Zapl.";
                                                        TASK_dat.Rows.Add(rw);
                                                        ind_DTA++;
                                                        if (MAX_Dta <= ind_DTA) { break; }
                                                    }
                                                    if (MAX_Dta > ind_DTA)
                                                    {
                                                        if ((DateTime)dta.DefaultView[ind_DTA].Row["DATA_DOST"] == Date_reQ && Part_no == dta.DefaultView[ind_DTA].Row["indeks"].ToString())
                                                        {
                                                            nedupdt = false;
                                                            tstchk = true;
                                                            if (Chk_Sum != Convert.ToDouble(dta.DefaultView[ind_DTA].Row["chk"]) || STAN_mag != Convert.ToDouble(dta.DefaultView[ind_DTA].Row["mag"]) || dta.DefaultView[ind_DTA].Row["typ_zdarzenia"].ToString() != state || QTY_SUPPLY != Convert.ToDouble(dta.DefaultView[ind_DTA].Row["WLK_DOST"]) || SUM_QTY_SUPPLY != Convert.ToDouble(dta.DefaultView[ind_DTA].Row["sum_dost"]) || SUM_QTY_DEMAND != Convert.ToDouble(dta.DefaultView[ind_DTA].Row["sum_potrz"]))
                                                            {
                                                                nedupdt = true;
                                                            }
                                                            else
                                                            {
                                                                ind_DTA++;
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                            if (balance < 0)
                                            {
                                                rpt_short = Date_reQ;
                                            }
                                            if (nedupdt)
                                            {
                                                DataRow row = TASK_dat.NewRow();
                                                row["indeks"] = Part_no;
                                                row["OPIS"] = (string)StMag.DefaultView[ind_mag].Row["OPIS"];
                                                row["KOLEKCJA"] = (string)StMag.DefaultView[ind_mag].Row["KOLEKCJA"];
                                                row["MAG"] = STAN_mag;
                                                row["PLANNER_BUYER"] = KOOR;
                                                row["RODZAJ"] = (string)StMag.DefaultView[ind_mag].Row["RODZAJ"];
                                                row["CZAS_DOSTAWY"] = (decimal)StMag.DefaultView[ind_mag].Row["CZAS_DOSTAWY"];
                                                row["DATA_GWARANCJI"] = gwar_DT;
                                                row["PRZYCZYNA"] = TYP_dmd;
                                                row["DATA_DOST"] = Date_reQ;
                                                row["WLK_DOST"] = QTY_SUPPLY;
                                                row["bilans"] = balance;
                                                row["bil_dost_dzień"] = bilans;
                                                if (Data_Braku != nullDAT)
                                                {
                                                    row["DATA_BRAKU"] = Data_Braku;
                                                }
                                                else
                                                {
                                                    row["DATA_BRAKU"] = Date_reQ;
                                                }
                                                row["typ_zdarzenia"] = state;
                                                row["widoczny_od_dnia"] = MAX_DEMANDs > ind_Demands ? DEMANDS.DefaultView[ind_Demands].Row["DAT_SHORTAGE"] ?? widoczny : start;
                                                row["sum_dost"] = SUM_QTY_SUPPLY;
                                                row["sum_potrz"] = SUM_QTY_DEMAND;
                                                row["sum_dost_opóźnion"] = Sum_dost_op;
                                                row["sum_potrz_opóźnion"] = Sum_potrz_op;
                                                row["Status_informacji"] = "NOT IMPLEMENT";
                                                row["refr_date"] = start;
                                                if (MAX_Dta > ind_DTA)
                                                {
                                                    if (tstchk)
                                                    {
                                                        row["Czynnosc"] = "MoD_DEM";
                                                        row["Status"] = "Zapl.";
                                                        row["Src_id"] = dta.DefaultView[ind_DTA].Row["id"];
                                                        ind_DTA++;

                                                    }
                                                    else
                                                    {
                                                        row["Czynnosc"] = "ADD_NEW_DEM";
                                                        row["Status"] = "Zapl.";
                                                    }
                                                }
                                                else
                                                {
                                                    row["Czynnosc"] = "ADD_NEW_DEM";
                                                    row["Status"] = "Zapl.";
                                                }
                                                row["chk"] = Chk_Sum;
                                                TASK_dat.Rows.Add(row);
                                            }
                                        }
                                    }
                                    #endregion
                                    #region Brak zamówień zakupu
                                    else
                                    {
                                        if (bilans < 0)
                                        {
                                            if (Part_no != NEXT_row["Part_no"].ToString() || (Date_reQ <= gwar_DT && (DateTime)NEXT_row["DATE_REQUIRED"] > gwar_DT))
                                            {
                                                bool tstchk = false;
                                                bool nedupdt = true;
                                                string state = Date_reQ <= gwar_DT ? "Braki w gwarantowanej dacie" : "Brak zamówień zakupu";
                                                if (Date_reQ <= gwar_DT)
                                                {
                                                    rpt_short = Date_reQ;
                                                }
                                                if (MAX_Dta > ind_DTA)
                                                {
                                                    if (Part_no == dta.DefaultView[ind_DTA].Row["indeks"].ToString())
                                                    {
                                                        while ((DateTime)dta.DefaultView[ind_DTA].Row["DATA_DOST"] < Date_reQ && Part_no == dta.DefaultView[ind_DTA].Row["indeks"].ToString())
                                                        {
                                                            DataRow rw = TASK_dat.NewRow();
                                                            rw["indeks"] = Part_no;
                                                            rw["DATA_DOST"] = Date_reQ;
                                                            rw["Src_id"] = dta.DefaultView[ind_DTA].Row["ID"];
                                                            rw["Czynnosc"] = "DEL_ID_DEM";
                                                            rw["Status"] = "Zapl.";
                                                            TASK_dat.Rows.Add(rw);
                                                            ind_DTA++;
                                                            if (MAX_Dta <= ind_DTA) { break; }
                                                        }
                                                        if (MAX_Dta > ind_DTA)
                                                        {
                                                            if ((DateTime)dta.DefaultView[ind_DTA].Row["DATA_DOST"] == Date_reQ && Part_no == dta.DefaultView[ind_DTA].Row["indeks"].ToString())
                                                            {
                                                                nedupdt = false;
                                                                tstchk = true;
                                                                if (Chk_Sum != Convert.ToDouble(dta.DefaultView[ind_DTA].Row["chk"]) || STAN_mag != Convert.ToDouble(dta.DefaultView[ind_DTA].Row["mag"]) || dta.DefaultView[ind_DTA].Row["typ_zdarzenia"].ToString() != state || QTY_SUPPLY != Convert.ToDouble(dta.DefaultView[ind_DTA].Row["WLK_DOST"]) || SUM_QTY_SUPPLY != Convert.ToDouble(dta.DefaultView[ind_DTA].Row["sum_dost"]) || SUM_QTY_DEMAND != Convert.ToDouble(dta.DefaultView[ind_DTA].Row["sum_potrz"]))
                                                                {
                                                                    nedupdt = true;
                                                                }
                                                                else
                                                                {
                                                                    ind_DTA++;
                                                                }

                                                            }
                                                        }
                                                    }
                                                }
                                                if (nedupdt)
                                                {
                                                    DataRow row = TASK_dat.NewRow();
                                                    row["indeks"] = Part_no;
                                                    row["OPIS"] = (string)StMag.DefaultView[ind_mag].Row["OPIS"];
                                                    row["KOLEKCJA"] = (string)StMag.DefaultView[ind_mag].Row["KOLEKCJA"];
                                                    row["MAG"] = STAN_mag;
                                                    row["PLANNER_BUYER"] = KOOR;
                                                    row["RODZAJ"] = (string)StMag.DefaultView[ind_mag].Row["RODZAJ"];
                                                    row["CZAS_DOSTAWY"] = (decimal)StMag.DefaultView[ind_mag].Row["CZAS_DOSTAWY"];
                                                    row["DATA_GWARANCJI"] = gwar_DT;
                                                    row["PRZYCZYNA"] = TYP_dmd;
                                                    row["DATA_DOST"] = Date_reQ;
                                                    row["WLK_DOST"] = QTY_SUPPLY;
                                                    row["bilans"] = bilans;
                                                    row["bil_dost_dzień"] = bilans;
                                                    if (Data_Braku != nullDAT)
                                                    {
                                                        row["DATA_BRAKU"] = Data_Braku;
                                                    }
                                                    else
                                                    {
                                                        row["DATA_BRAKU"] = Date_reQ;
                                                    }
                                                    row["typ_zdarzenia"] = state;
                                                    row["widoczny_od_dnia"] = MAX_DEMANDs > ind_Demands ? DEMANDS.DefaultView[ind_Demands].Row["DAT_SHORTAGE"] ?? widoczny : start;
                                                    row["sum_dost"] = SUM_QTY_SUPPLY;
                                                    row["sum_potrz"] = SUM_QTY_DEMAND;
                                                    row["sum_dost_opóźnion"] = Sum_dost_op;
                                                    row["sum_potrz_opóźnion"] = Sum_potrz_op;
                                                    row["Status_informacji"] = "NOT IMPLEMENT";
                                                    row["refr_date"] = start;
                                                    if (MAX_Dta > ind_DTA)
                                                    {
                                                        if (tstchk)
                                                        {
                                                            row["Czynnosc"] = "MoD_DEM";
                                                            row["Status"] = "Zapl.";
                                                            row["Src_id"] = dta.DefaultView[ind_DTA].Row["id"];
                                                            ind_DTA++;
                                                        }
                                                        else
                                                        {
                                                            row["Czynnosc"] = "ADD_NEW_DEM";
                                                            row["Status"] = "Zapl.";
                                                        }
                                                    }
                                                    else
                                                    {
                                                        row["Czynnosc"] = "ADD_NEW_DEM";
                                                        row["Status"] = "Zapl.";
                                                    }
                                                    row["chk"] = Chk_Sum;
                                                    TASK_dat.Rows.Add(row);
                                                }
                                            }
                                        }
                                        else
                                        {
                                            if (MAX_Dta > ind_DTA)
                                            {
                                                while ((DateTime)dta.DefaultView[ind_DTA].Row["DATA_DOST"] == Date_reQ && Part_no == dta.DefaultView[ind_DTA].Row["indeks"].ToString())
                                                {
                                                    DataRow rw = TASK_dat.NewRow();
                                                    rw["indeks"] = Part_no;
                                                    rw["DATA_DOST"] = Date_reQ;
                                                    rw["Src_id"] = dta.DefaultView[ind_DTA].Row["ID"];
                                                    rw["Czynnosc"] = "DEL_ID_DEM";
                                                    rw["Status"] = "Zapl.";
                                                    TASK_dat.Rows.Add(rw);
                                                    ind_DTA++;
                                                    if (MAX_Dta <= ind_DTA) { break; }
                                                }
                                            }
                                        }
                                    }
                                    #endregion
                                    #region Skasuj dalsze daty po indeksie
                                    if (MAX_Dta > ind_DTA)
                                    {
                                        if (Part_no != NEXT_row["Part_no"].ToString())
                                        {
                                            while ((DateTime)dta.DefaultView[ind_DTA].Row["DATA_DOST"] > Date_reQ && Part_no == dta.DefaultView[ind_DTA].Row["indeks"].ToString())
                                            {
                                                DataRow rw = TASK_dat.NewRow();
                                                rw["indeks"] = Part_no;
                                                rw["DATA_DOST"] = Date_reQ;
                                                rw["Src_id"] = dta.DefaultView[ind_DTA].Row["ID"];
                                                rw["Czynnosc"] = "DEL_ID_DEM";
                                                rw["Status"] = "Zapl.";
                                                TASK_dat.Rows.Add(rw);
                                                ind_DTA++;
                                                if (MAX_Dta <= ind_DTA) { break; }
                                            }
                                        }
                                    }
                                    #endregion
                                    if ((STAN_mag + SUM_QTY_SUPPLY - SUM_QTY_DEMAND >= 0 && Date_reQ > DATNOW) && Data_Braku != nullDAT)
                                    {
                                        Data_Braku = nullDAT;
                                    }
                                }
                                catch (Exception e)
                                {
                                    using (NpgsqlConnection conA = new NpgsqlConnection(npC))
                                    {
                                        await conA.OpenAsync();
                                        using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                                            "UPDATE public.datatbles " +
                                            "SET in_progress=false,updt_errors=true " +
                                            "WHERE table_name='main_loop'", conA))
                                        {
                                            cmd.ExecuteNonQuery();
                                        }
                                        conA.Close();
                                    }
                                    Log("Błąd obliczania tabeli informcyjnej zakupy :" + e);
                                    return 1;
                                }
                                #endregion
                            }
                            try
                            {
                                while (ind_mag < StMag.Rows.Count - 1 && (MAX_Dta > ind_DTA || MAX_DEMANDs > ind_Demands))
                                {
                                    DataRow row = eras_Shedul.NewRow();
                                    row["part_no"] = StMag.DefaultView[ind_mag].Row["Indeks"].ToString();
                                    eras_Shedul.Rows.Add(row);

                                    row = TASK_DEMANDS.NewRow();
                                    row["PART_NO"] = StMag.DefaultView[ind_mag].Row["Indeks"].ToString();
                                    row["Czynnosc"] = "DEL_PART_DEM";
                                    row["Status"] = "Zapl.";
                                    TASK_DEMANDS.Rows.Add(row);

                                    row = TASK_dat.NewRow();
                                    row["indeks"] = StMag.DefaultView[ind_mag].Row["Indeks"].ToString();
                                    row["Czynnosc"] = "DEL_PART_DEM";
                                    row["Status"] = "Zapl.";
                                    TASK_dat.Rows.Add(row);
                                    ind_mag++;
                                }
                            }
                            catch (Exception e)
                            {
                                Log("Błąd podczas przypisywania tablicy zleceń do usunięcia:" + e);
                            }
                            #region Wyczyść niepotrzebne dane
                            try
                            {
                                StMag.Clear();
                                DMND_ORA.Clear();
                                DEMANDS.Clear();
                                dta.Clear();
                            }
                            catch (Exception e)
                            {
                                Log("Błąd czyszczenia tablic danych MAIN LOOP:" + e);
                            }
                            #endregion
                            Log("Zakończono główną pętlę");
                            #region Asynchroniczne wywołania funkcji
                            int tsk = 0;
                            int tsk1 = 0;
                            int dtaOrd = 0;
                            int tsk3 = 0;
                            int tsk2 = 0;
                            int tsk4 = 0;
                            int tsk5 = 0;
                            try
                            {
                                GC.Collect();
                                MShedul.DefaultView.Sort = "part_no,dat";
                                using (NpgsqlConnection conA = new NpgsqlConnection(npC))
                                {
                                    await conA.OpenAsync();
                                    using (NpgsqlTransaction TR_settables = conA.BeginTransaction())
                                    {
                                        using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                                            "UPDATE public.datatbles	" +
                                            "SET start_update=current_timestamp, in_progress=true " +
                                            "WHERE table_name='worker_all'", conA))
                                        {
                                            cmd.ExecuteNonQuery();
                                        }
                                        using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                                            "UPDATE public.datatbles	" +
                                            "SET start_update=current_timestamp, in_progress=true " +
                                            "WHERE table_name='ord_demands'", conA))
                                        {
                                            cmd.ExecuteNonQuery();
                                        }
                                        if (eras_Shedul.Rows.Count > 0)
                                        {
                                            using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                                                "UPDATE public.datatbles	" +
                                                "SET start_update=current_timestamp, in_progress=true " +
                                                "WHERE table_name='wrk_del'", conA))
                                            {
                                                cmd.ExecuteNonQuery();
                                            }
                                        }
                                        TR_settables.Commit();
                                    }
                                    conA.Close();
                                }
                                if (eras_Shedul.Rows.Count > 0)
                                {
                                    try
                                    {
                                        System.Threading.Thread.Sleep(2000);
                                        DataRow[] dt = eras_Shedul.Select("Dat is not null");
                                        Log("RECORDS DELETE_DOP_ord: " + dt.Length);
                                        using (NpgsqlConnection conA = new NpgsqlConnection(npA))
                                        {
                                            conA.Open();
                                            using (NpgsqlTransaction TR_Dem = conA.BeginTransaction())
                                            {
                                                using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                                                    "DELETE " +
                                                    "FROM public.ord_demands " +
                                                    "WHERE part_no=@part_no AND date_required=@dat;", conA))
                                                {
                                                    cmd.Parameters.Add("@part_no", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                    cmd.Parameters.Add("@dat", NpgsqlTypes.NpgsqlDbType.Timestamp);
                                                    cmd.Prepare();
                                                    foreach (DataRow row in dt)
                                                    {
                                                        cmd.Parameters[0].Value = row[0];
                                                        cmd.Parameters[1].Value = (DateTime)row[1];
                                                        cmd.ExecuteNonQuery();
                                                        //Log("DELETE_PART_DAT_DOP: " + row[0] + " DATA:" + row[1]);
                                                    }
                                                    //Log("END DELETE_GIUD__DOP_ord: " + (DateTime.Now - start));
                                                }
                                                dt = eras_Shedul.Select("Dat is null");
                                                Log("RECORDS DELETE_DOP_part: " + dt.Length);
                                                using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                                                    "DELETE " +
                                                    "FROM public.ord_demands " +
                                                    "WHERE part_no=@part_no;", conA))
                                                {
                                                    cmd.Parameters.Add("@part_no", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                    cmd.Prepare();
                                                    foreach (DataRow row in dt)
                                                    {
                                                        cmd.Parameters[0].Value = row[0];
                                                        cmd.ExecuteNonQuery();
                                                        //Log("DELETE_PART_DOP: " + row[0]);
                                                    }
                                                    Log("END DELETE_GIUD__DOP_ord: " + (DateTime.Now - start));

                                                }
                                                using (NpgsqlCommand cmd1 = new NpgsqlCommand("" +
                                                    "UPDATE public.datatbles " +
                                                    "SET last_modify=current_timestamp, in_progress=false,updt_errors=false " +
                                                    "WHERE table_name='wrk_del'", conA))
                                                {
                                                    cmd1.ExecuteNonQuery();
                                                }
                                                TR_Dem.Commit();
                                            }
                                        }
                                    }
                                    catch (Exception e)
                                    {
                                        Log("Błąd kasowania niepotrzebnych zapisów w ord_demands:" + e);
                                    }
                                }
                                using (NpgsqlConnection conA = new NpgsqlConnection(npA))
                                {
                                    conA.Open();
                                    using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                                        "select cast(count(table_name) as integer) busy " +
                                        "from public.datatbles " +
                                        "where table_name='wrk_del' and in_progress=true", conA))
                                    {
                                        int busy_il = 1;
                                        while (busy_il > 0)
                                        {
                                            busy_il = Convert.ToInt16(cmd.ExecuteScalar());
                                            if (busy_il > 0) { System.Threading.Thread.Sleep(250); }
                                        }
                                    }
                                }
                                Parallel.Invoke(srv_op, async () => dtaOrd = await Get_ord_dem(), async () => tsk3 = await Get_ord_dem1(), async () => tsk2 = await Get_ord_dem2(), async () => tsk4 = await Get_ord_dem3(), async () => tsk5 = await Get_ord_dem4(), async () => tsk = await DO_JOB_DEM(), async () => tsk1 = await DO_JOB_dta());
                                if (tsk == 1) { tsk = await DO_JOB_DEM(); }
                                if (tsk1 == 1) { tsk1 = await DO_JOB_dta(); }
                                TMPTASK_dat.Clear();
                                int chthre = dtaOrd + tsk3 + tsk2 + tsk4;
                                if (chthre > 1)
                                {
                                    Log("Błąd Coś nie zadziałało powtórka wywołania");
                                    if (chthre == 4)
                                    {
                                        Parallel.Invoke(srv_op, async () => dtaOrd = await Get_ord_dem(), async () => tsk3 = await Get_ord_dem1(), async () => tsk2 = await Get_ord_dem2(), async () => tsk4 = await Get_ord_dem3());
                                    }
                                    else if (chthre == 3)
                                    {
                                        if (dtaOrd == 0) { Parallel.Invoke(srv_op, async () => tsk3 = await Get_ord_dem1(), async () => tsk2 = await Get_ord_dem2(), async () => tsk4 = await Get_ord_dem3()); }
                                        if (tsk3 == 0) { Parallel.Invoke(srv_op, async () => dtaOrd = await Get_ord_dem(), async () => tsk2 = await Get_ord_dem2(), async () => tsk4 = await Get_ord_dem3()); }
                                        if (tsk2 == 0) { Parallel.Invoke(srv_op, async () => dtaOrd = await Get_ord_dem(), async () => tsk3 = await Get_ord_dem1(), async () => tsk4 = await Get_ord_dem3()); }
                                        if (tsk4 == 0) { Parallel.Invoke(srv_op, async () => dtaOrd = await Get_ord_dem(), async () => tsk3 = await Get_ord_dem1(), async () => tsk2 = await Get_ord_dem2()); }
                                    }
                                    else if (chthre == 2)
                                    {
                                        if (tsk2 == 1)
                                        {
                                            if (tsk4 == 1)
                                            {
                                                Parallel.Invoke(srv_op, async () => tsk2 = await Get_ord_dem2(), async () => tsk4 = await Get_ord_dem3());
                                            }
                                            else if (tsk3 == 1)
                                            {
                                                Parallel.Invoke(srv_op, async () => tsk3 = await Get_ord_dem1(), async () => tsk2 = await Get_ord_dem2());
                                            }
                                            else if (dtaOrd == 1)
                                            {
                                                Parallel.Invoke(srv_op, async () => dtaOrd = await Get_ord_dem(), async () => tsk2 = await Get_ord_dem2());
                                            }
                                        }
                                        else if (tsk4 == 1)
                                        {
                                            if (tsk3 == 1)
                                            {
                                                Parallel.Invoke(srv_op, async () => tsk3 = await Get_ord_dem1(), async () => tsk4 = await Get_ord_dem3());
                                            }
                                            else if (dtaOrd == 1)
                                            {
                                                Parallel.Invoke(srv_op, async () => dtaOrd = await Get_ord_dem(), async () => tsk4 = await Get_ord_dem3());
                                            }
                                        }
                                        else
                                        {
                                            Parallel.Invoke(srv_op, async () => dtaOrd = await Get_ord_dem(), async () => tsk3 = await Get_ord_dem1());
                                        }
                                    }
                                    else
                                    {
                                        if (dtaOrd == 1) { dtaOrd = await Get_ord_dem(); }
                                        if (tsk3 == 1) { tsk3 = await Get_ord_dem1(); }
                                        if (tsk2 == 1) { tsk2 = await Get_ord_dem2(); }
                                        if (tsk4 == 1) { tsk4 = await Get_ord_dem3(); }
                                    }
                                }
                                // JEŚLI BŁĄD TO JESZCZE RAZ ALE POJEDYŃCZO

                                if (tsk5 == 1) { tsk5 = await Get_ord_dem4(); }
                                if (dtaOrd == 1) { dtaOrd = await Get_ord_dem(); }
                                if (tsk3 == 1) { tsk3 = await Get_ord_dem1(); }
                                if (tsk2 == 1) { tsk2 = await Get_ord_dem2(); }
                                if (tsk4 == 1) { tsk4 = await Get_ord_dem3(); }
                                if (tsk == 1) { tsk = await DO_JOB_DEM(); }
                                if (tsk1 == 1) { tsk1 = await DO_JOB_dta(); }
                            }
                            catch (Exception e)
                            {
                                Log("Błąd wywołania funkcji zbierających/zapisujących dane:" + e);
                            }
                            try
                            {
                                using (NpgsqlConnection conA = new NpgsqlConnection(npA))
                                {
                                    conA.Open();
                                    using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                                        "select cast(count(table_name) as integer) busy " +
                                        "from public.datatbles " +
                                        "where ((substring(table_name,1,6)='worker' and table_name!='worker_all') or table_name='demands') and in_progress=true", conA))
                                    {
                                        int busy_il = 1;
                                        while (busy_il > 0)
                                        {
                                            busy_il = Convert.ToInt16(cmd.ExecuteScalar());
                                            if (busy_il > 0) { System.Threading.Thread.Sleep(250); }
                                        }
                                    }
                                }
                                try
                                {
                                    using (NpgsqlConnection conA = new NpgsqlConnection(npA))
                                    {
                                        conA.Open();
                                        using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                                            "select cast(count(table_name) as integer) busy f" +
                                            "rom public.datatbles " +
                                            "where table_name='wrk_del' and in_progress=true", conA))
                                        {
                                            int busy_il = 1;
                                            while (busy_il > 0)
                                            {
                                                busy_il = Convert.ToInt16(cmd.ExecuteScalar());
                                                if (busy_il > 0) { System.Threading.Thread.Sleep(250); }
                                            }
                                        }
                                    }
                                    using (NpgsqlConnection conA = new NpgsqlConnection(npC))
                                    {
                                        await conA.OpenAsync();
                                        using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                                            "UPDATE public.datatbles	" +
                                            "SET start_update=current_timestamp, in_progress=true " +
                                            "WHERE table_name='potw'", conA))
                                        {
                                            cmd.ExecuteNonQuery();
                                        }
                                        conA.Close();
                                    }
                                    using (NpgsqlConnection conA = new NpgsqlConnection(npA))
                                    {
                                        await conA.OpenAsync();
                                        using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                                            "select cast(count(table_name) as integer) busy " +
                                            "from public.datatbles " +
                                            "where ((substring(table_name,1,6)='worker' and table_name!='worker_all') or table_name='demands') and in_progress=true", conA))
                                        {
                                            int busy_il = 1;
                                            while (busy_il > 0)
                                            {
                                                busy_il = Convert.ToInt16(cmd.ExecuteScalar());
                                                if (busy_il > 0) { System.Threading.Thread.Sleep(250); }
                                            }
                                        }
                                        conA.Close();
                                    }
                                    using (NpgsqlConnection conA = new NpgsqlConnection(npA))
                                    {
                                        conA.Open();

                                        using (NpgsqlTransaction TR_info = conA.BeginTransaction(IsolationLevel.ReadCommitted))
                                        {
                                            using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                                                "UPDATE public.datatbles " +
                                                "SET last_modify=current_timestamp, in_progress=false,updt_errors=false " +
                                                "WHERE table_name='ord_demands'", conA))
                                            {
                                                cmd.ExecuteNonQuery();
                                            }
                                            using (NpgsqlCommand cmd = new NpgsqlCommand("REFRESH MATERIALIZED VIEW bilans_val", conA))
                                            {
                                                cmd.ExecuteNonQuery();
                                            }
                                            using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                                                "UPDATE public.datatbles " +
                                                "SET last_modify=current_timestamp, in_progress=false,updt_errors=false " +
                                                "WHERE table_name='potw'", conA))
                                            {
                                                cmd.ExecuteNonQuery();
                                            }
                                            using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                                                "UPDATE public.datatbles " +
                                                "SET last_modify=current_timestamp, in_progress=false,updt_errors=false " +
                                                "WHERE table_name='main_loop'", conA))
                                            {
                                                cmd.ExecuteNonQuery();
                                            }
                                            TR_info.Commit();
                                        }
                                    }
                                }
                                catch (Exception e)
                                {
                                    Log("Błąd aktualizacji formatki dla działu zakupy:" + e);
                                }
                            }
                            catch (Exception e)
                            {
                                Log("Błąd funkcji sprawdzających poprawność danych z rozproszonych procesów:" + e);
                            }

                            #endregion
                            #region Uzupełnij i zaktualizuj formatkę z zagrożeniami w zleceniach i DOP
                            try
                            {
                                GC.Collect();
                                using (NpgsqlConnection conA = new NpgsqlConnection(npC))
                                {
                                    await conA.OpenAsync();
                                    using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                                        "select cast(count(table_name) as integer) busy " +
                                        "from public.datatbles " +
                                        "where ((substring(table_name,1,6)='worker' and table_name!='worker_all') or table_name='demands' or table_name='main_loop') and in_progress=true", conA))
                                    {
                                        int busy_il = 1;
                                        while (busy_il > 0)
                                        {
                                            busy_il = Convert.ToInt16(cmd.ExecuteScalar());
                                            if (busy_il > 0) { System.Threading.Thread.Sleep(250); }
                                        }
                                    }
                                    using (NpgsqlTransaction TR_ORDdem = conA.BeginTransaction(IsolationLevel.ReadCommitted))
                                    {
                                        using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                                            "DELETE " +
                                            "FROM public.ord_demands " +
                                            "where " +
                                            "ord_demands.id in " +
                                                "(select id from((select ord_demands.id,part_no,date_required from ord_demands) a " +
                                                "left join " +
                                                "(select part_no,work_day from demands) b " +
                                                "on b.part_no=a.part_no and b.work_day=a.date_required) " +
                                            "where work_day is null);", conA))
                                        {
                                            Log("ord_demands del" + (DateTime.Now - start));
                                            cmd.ExecuteNonQuery();
                                        }
                                        using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                                            "update demands " +
                                            "set \"inDB\"=up.chk_in " +
                                            "from " +
                                             "(select id,chk_in " +
                                                "from (select a.id,a.part_no,a.work_day,a.indb,CASE WHEN b.part_no is null THEN false ELSE true END as chk_in " +
                                                    "from (select id,part_no,work_day,\"inDB\" as indb from demands) a " +
                                                    "left join " +
                                                    "(select part_no,date_required from ord_demands group by part_no,date_required) b " +
                                                    "on b.part_no=a.part_no and b.date_required=a.work_day) as b " +
                                                 "where indb is null or indb!=chk_in) as up " +
                                             "where demands.id=up.id;", conA))
                                        {
                                            Log("Demands update inDb" + (DateTime.Now - start));
                                            cmd.ExecuteNonQuery();
                                        }
                                        TR_ORDdem.Commit();
                                    }
                                    MShedul.Clear();
                                    eras_Shedul.Clear();

                                    using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                                        "select b.part_no,b.date_required as dat,true Purch_rpt,b.date_required Purch_rpt_dat,false Braki_rpt,b.date_required Braki_rpt_dat,0 kol " +
                                        "from " +
                                            "(select dop,dop_lin,int_ord,part_no,line_no,rel_no,count(order_supp_dmd) as il " +
                                                "from ord_demands " +
                                                "group by dop,dop_lin,int_ord,part_no,line_no,rel_no) a," +
                                            "(select part_no,date_required,dop,dop_lin,int_ord,line_no,rel_no from ord_demands) b " +
                                         "where a.il >1 and (a.dop=b.dop and a.dop_lin=b.dop_lin and a.int_ord=b.int_ord and a.part_no=b.part_no " +
                                            "and a.line_no=b.line_no and a.rel_no=b.rel_no) " +
                                         "group by b.part_no,b.date_required order by part_no,dat", conA))
                                    {
                                        using (NpgsqlDataReader po = cmd.ExecuteReader())
                                        {
                                            MShedul.Load(po);
                                        }
                                    }
                                    if (MShedul.Rows.Count > 0)
                                    {
                                        dtaOrd = await Get_ord_dem();
                                        using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                                            "update demands " +
                                            "set \"inDB\"=up.chk_in " +
                                            "from " +
                                                "(select id,chk_in " +
                                                    "from " +
                                                    "(select a.id,a.part_no,a.work_day,a.indb,CASE WHEN b.part_no is null THEN false ELSE true END as chk_in " +
                                                        "from (select id,part_no,work_day,\"inDB\" as indb from demands) a " +
                                                        "left join " +
                                                        "(select part_no,date_required from ord_demands group by part_no,date_required) b " +
                                                        "on b.part_no=a.part_no and b.date_required=a.work_day) as b " +
                                                     "where indb is null or indb!=chk_in) as up " +
                                                "where demands.id=up.id;", conA))
                                        {
                                            Log("Demands update inDb" + (DateTime.Now - start));
                                            cmd.ExecuteNonQuery();
                                        }
                                    }
                                    using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                                        "select indeks,mn,ma,max(data_dost) dost " +
                                        "from " +
                                            "(select c.indeks,c.data_dost,c.mag,c.sum_dost-sum(a.qty_supply) supp,c.sum_potrz-sum(a.qty_demand) dmd,min(a.date_required) mn,b.ma " +
                                                "from " +
                                                    "(select * " +
                                                        "from public.data " +
                                                        "where typ_zdarzenia not in ('Brak zamówień zakupu','Dostawa na dzisiejsze ilości','Opóźniona dostawa') " +
                                                        "and planner_buyer!='LUCPRZ')c," +
                                                    "ord_demands a," +
                                                    "(select part_no,min(work_day) ma " +
                                                        "from demands group by part_no) b " +
                                                 "where c.bilans<0 and b.part_no=c.indeks and a.part_no=c.indeks " +
                                                 "and (a.date_required<=c.data_dost or a.date_required<=current_date) " +
                                                 "group by c.indeks,c.opis,c.data_dost,c.bilans,c.mag,c.sum_dost,c.sum_potrz,b.ma) a " +
                                            "where  supp not between -0.001 and 0.001 or dmd not between -0.001 and 0.001 group by indeks,mn,ma", conA))
                                    {
                                        using (NpgsqlDataReader po = cmd.ExecuteReader())
                                        {
                                            using (DataTable tmpDT = new DataTable())
                                            {
                                                tmpDT.Load(po);
                                                if (tmpDT.Rows.Count > 0)
                                                {
                                                    using (OracleConnection conO = new OracleConnection("Password = pass;User ID = user; Data Source = prod8"))
                                                    {
                                                        await conO.OpenAsync();
                                                        MShedul.Clear();
                                                        using (NpgsqlConnection con = new NpgsqlConnection(npA))
                                                        {
                                                            con.Open();
                                                            using (NpgsqlCommand cmd2 = new NpgsqlCommand("update public.data set mag=@mag where indeks=@part_no", con))
                                                            {
                                                                cmd2.Parameters.Add("mag", NpgsqlTypes.NpgsqlDbType.Double);
                                                                cmd2.Parameters.Add("@part_no", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                                cmd2.Prepare();
                                                                using (NpgsqlCommand cmd3 = new NpgsqlCommand("update public.data set mag=@mag where indeks=@part_no", con))
                                                                {
                                                                    cmd3.Parameters.Add("mag", NpgsqlTypes.NpgsqlDbType.Double);
                                                                    cmd3.Parameters.Add("@part_no", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                                    cmd3.Prepare();
                                                                    using (OracleCommand cmd1 = new OracleCommand("" +
                                                                        "select  ifsapp.inventory_part_in_stock_api. Get_Plannable_Qty_Onhand ('ST',:part_no,'*') from dual", conO))
                                                                    {
                                                                        cmd1.Parameters.Add(":part_no", OracleDbType.Varchar2);
                                                                        cmd1.Prepare();
                                                                        foreach (DataRowView rw in tmpDT.DefaultView)
                                                                        {
                                                                            kol++;
                                                                            if (kol == 5) { kol = 0; }
                                                                            DataRow row = MShedul.NewRow();
                                                                            row["part_no"] = rw["indeks"];
                                                                            row["dat"] = (DateTime)rw["mn"] > (DateTime)rw["ma"] ? (DateTime)rw["ma"] : (DateTime)rw["mn"];
                                                                            row["purch_rpt"] = true;
                                                                            row["purch_rpt_dat"] = (DateTime)rw["mn"] > (DateTime)rw["ma"] ? (DateTime)rw["ma"] : (DateTime)rw["mn"];
                                                                            row["braki_rpt"] = false;
                                                                            row["braki_rpt_dat"] = (DateTime)rw["ma"];
                                                                            row["kol"] = kol;
                                                                            MShedul.Rows.Add(row);

                                                                            row = MShedul.NewRow();
                                                                            row["part_no"] = rw["indeks"];
                                                                            row["dat"] = rw["dost"];
                                                                            row["purch_rpt"] = true;
                                                                            row["purch_rpt_dat"] = (DateTime)rw["mn"] > (DateTime)rw["ma"] ? (DateTime)rw["ma"] : (DateTime)rw["mn"];
                                                                            row["braki_rpt"] = false;
                                                                            row["braki_rpt_dat"] = (DateTime)rw["ma"];
                                                                            row["kol"] = kol;
                                                                            MShedul.Rows.Add(row);

                                                                            cmd1.Parameters[0].Value = rw["indeks"];
                                                                            double inq = Convert.ToDouble(cmd1.ExecuteScalar());
                                                                            cmd2.Parameters[0].Value = inq;
                                                                            cmd2.Parameters[1].Value = rw["indeks"];
                                                                            cmd2.ExecuteNonQuery();
                                                                            cmd3.Parameters[0].Value = inq;
                                                                            cmd3.Parameters[1].Value = rw["indeks"];
                                                                            cmd3.ExecuteNonQuery();
                                                                        }
                                                                    }
                                                                }
                                                            }
                                                            using (NpgsqlCommand cmd1 = new NpgsqlCommand("" +
                                                                "UPDATE public.data b " +
                                                                "SET status_informacji=a.potw,informacja=a.info " +
                                                                "from " +
                                                                    "(select a.id," +
                                                                    "case when a.typ_zdarzenia in ('Dzisiejsza dostawa','Opóźniona dostawa','Nieaktualne Dostawy') " +
                                                                        "then null else coalesce(b.rodzaj_potw,'BRAK') end potw," +
                                                                    "a.status_informacji," +
                                                                    "b.info " +
                                                                    "from " +
                                                                        "public.data a " +
                                                                        "left join " +
                                                                        "potw b " +
                                                                        "on b.indeks=a.indeks and (b.data_dost=a.data_dost or b.rodzaj_potw='NIE ZAMAWIAM') " +
                                                                     "where coalesce (a.status_informacji,'N')!=coalesce(case when a.typ_zdarzenia " +
                                                                        "in ('Dzisiejsza dostawa','Opóźniona dostawa','Nieaktualne Dostawy') then null " +
                                                                        "else coalesce(b.rodzaj_potw,'BRAK') end,'N') or " +
                                                                            "coalesce(a.informacja,'n')!=coalesce(b.info,'n'))  a  " +
                                                                 "where b.id=a.id", conA))
                                                            {
                                                                cmd1.ExecuteNonQuery();
                                                            }
                                                        }
                                                        Parallel.Invoke(srv_op, async () => dtaOrd = await Get_ord_dem(), async () => tsk3 = await Get_ord_dem1(), async () => tsk2 = await Get_ord_dem2(), async () => tsk4 = await Get_ord_dem3(), async () => tsk5 = await Get_ord_dem4());
                                                        using (NpgsqlCommand cmd1 = new NpgsqlCommand("" +
                                                            "update demands " +
                                                            "set \"inDB\"=up.chk_in " +
                                                            "from (select id,chk_in " +
                                                                "from " +
                                                                "(select a.id,a.part_no,a.work_day,a.indb,CASE WHEN b.part_no is null THEN false ELSE true END as chk_in " +
                                                                    "from " +
                                                                    "(select id,part_no,work_day,\"inDB\" as indb from demands) a " +
                                                                    "left join " +
                                                                    "(select part_no,date_required from ord_demands group by part_no,date_required) b " +
                                                                    "on b.part_no=a.part_no and b.date_required=a.work_day) as b " +
                                                                "where indb is null or indb!=chk_in) as up " +
                                                            "where demands.id=up.id;", conA))
                                                        {
                                                            Log("Demands update inDb" + (DateTime.Now - start));
                                                            cmd1.ExecuteNonQuery();
                                                        }
                                                        conO.Close();
                                                    }
                                                }
                                            }
                                        }
                                    }
                                    using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                                        "select cast(count(table_name) as integer) busy " +
                                        "from public.datatbles " +
                                        "where substring(table_name,1,6)='worker' and table_name!='worker_all' and in_progress=true", conA))
                                    {
                                        int busy_il = 1;
                                        while (busy_il > 0)
                                        {
                                            busy_il = Convert.ToInt16(cmd.ExecuteScalar());
                                            if (busy_il > 0) { System.Threading.Thread.Sleep(250); }
                                        }
                                    }
                                    using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                                        "select part_no ,min(date_required),max(date_required) " +
                                        "from " +
                                            "(select a.part_no,a.date_required,sum(a.qty_demand)-b.qty_demand div_dem,sum(a.qty_supply)-b.purch_qty div_purch " +
                                                "from ord_demands a " +
                                                "left join " +
                                                "demands b " +
                                                "on b.part_no=a.part_no and b.work_day=a.date_required " +
                                              "group by a.part_no,a.date_required,b.qty_demand,b.purch_qty order by part_no,date_required)a " +
                                          "where  (div_dem not between -0.00001 and 0.00001) or (div_purch not between -0.00001 and 0.00001) group by part_no", conA))
                                    {
                                        using (NpgsqlDataReader po = cmd.ExecuteReader())
                                        {
                                            MShedul.Clear();
                                            using (DataTable tmpDT = new DataTable())
                                            {
                                                tmpDT.Load(po);
                                                if (tmpDT.Rows.Count > 0)
                                                {
                                                    foreach (DataRowView rw in tmpDT.DefaultView)
                                                    {
                                                        kol++;
                                                        if (kol == 5) { kol = 0; }
                                                        DataRow row = MShedul.NewRow();
                                                        row["part_no"] = rw["part_no"];
                                                        row["dat"] = (DateTime)rw["min"];
                                                        row["purch_rpt"] = true;
                                                        row["purch_rpt_dat"] = (DateTime)rw["max"];
                                                        row["braki_rpt"] = true;
                                                        row["braki_rpt_dat"] = (DateTime)rw["min"];
                                                        row["kol"] = kol;
                                                        MShedul.Rows.Add(row);

                                                        row = MShedul.NewRow();
                                                        row["part_no"] = rw["part_no"];
                                                        row["dat"] = (DateTime)rw["max"];
                                                        row["purch_rpt"] = true;
                                                        row["purch_rpt_dat"] = (DateTime)rw["max"];
                                                        row["braki_rpt"] = true;
                                                        row["braki_rpt_dat"] = (DateTime)rw["min"];
                                                        row["kol"] = kol;
                                                        MShedul.Rows.Add(row);
                                                    }
                                                    Parallel.Invoke(srv_op, async () => dtaOrd = await Get_ord_dem(), async () => tsk3 = await Get_ord_dem1(), async () => tsk2 = await Get_ord_dem2(), async () => tsk4 = await Get_ord_dem3(), async () => tsk5 = await Get_ord_dem4());
                                                }
                                            }

                                        }
                                    }
                                    using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                                        "update demands " +
                                        "set \"inDB\"=up.chk_in " +
                                        "from (select id,chk_in " +
                                            "from (select a.id,a.part_no,a.work_day,a.indb,CASE WHEN b.part_no is null THEN false ELSE true END as chk_in " +
                                                "from (select id,part_no,work_day,\"inDB\" as indb from demands) a " +
                                                    "left join " +
                                                    "(select part_no,date_required from ord_demands group by part_no,date_required) b " +
                                                    "on b.part_no=a.part_no and b.date_required=a.work_day) as b " +
                                                 "where indb is null or indb!=chk_in) as up where demands.id=up.id;", conA))
                                    {
                                        Log("Demands update inDb" + (DateTime.Now - start));
                                        cmd.ExecuteNonQuery();
                                    }
                                    conA.Close();
                                    conA.Open();
                                    using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                                        "select cast(count(table_name) as integer) busy " +
                                        "from public.datatbles " +
                                        "where ((substring(table_name,1,6)='worker' and table_name!='worker_all')  or table_name='wrk_del') and in_progress=true", conA))
                                    {
                                        int busy_il = 1;
                                        while (busy_il > 0)
                                        {
                                            busy_il = Convert.ToInt16(cmd.ExecuteScalar());
                                            if (busy_il > 0) { System.Threading.Thread.Sleep(250); }
                                        }
                                    }
                                    using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                                        "UPDATE public.datatbles " +
                                        "SET last_modify=current_timestamp, in_progress=false,updt_errors=false " +
                                        "WHERE table_name='worker_all'", conA))
                                    {
                                        cmd.ExecuteNonQuery();
                                    }
                                    conA.Close();
                                }
                            }
                            catch (Exception e)
                            {
                                using (NpgsqlConnection conA = new NpgsqlConnection(npC))
                                {
                                    await conA.OpenAsync();
                                    using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                                        "UPDATE public.datatbles SET in_progress=false,updt_errors=true " +
                                        "WHERE table_name='ord_demands'", conA))
                                    {
                                        cmd.ExecuteNonQuery();
                                    }
                                    conA.Close();
                                }
                                Log("Błąd obliczeń :" + e);
                                return 1;
                            }

                            using (NpgsqlConnection conA = new NpgsqlConnection(npC))
                            {
                                await conA.OpenAsync();
                                using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                                    "UPDATE public.datatbles " +
                                    "SET last_modify=current_timestamp, in_progress=false,updt_errors=false " +
                                    "WHERE table_name='ord_demands'", conA))
                                {
                                    cmd.ExecuteNonQuery();
                                }
                                conA.Close();
                            }
                            Log("END:" + (DateTime.Now - start));
                            return 0;
                        }
                        else
                        {
                            Log("Błąd obliczeń w podrzędnych procesach ");
                            return 1;
                        }
                        #endregion
                    }
                    catch (Exception e)
                    {
                        Log("Błąd w module obliczeń głównych" + e);
                        return 1;
                    }
                }
                #endregion
            }
            catch (Exception e)
            {
                Log("Błąd w początkowym wywołaniu ,przypisaniu zmiennych :" + e);
            }
            finally
            {
                Log("END MAIN PROGRAM:" + (DateTime.Now - Serw_run));
                Save_stat_refr();
            }
        }
        #endregion
        #region Asynchroniczne wstępne ustawienia i pobranie danych 
        private async Task<int> Get_Kalendar()
        {
            try
            {
                using (NpgsqlConnection conA = new NpgsqlConnection(npC))
                {
                    await conA.OpenAsync();
                    using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                        "UPDATE public.datatbles " +
                        "SET start_update=current_timestamp, in_progress=true " +
                        "WHERE table_name='work_cal'", conA))
                    {
                        cmd.ExecuteNonQuery();
                    }
                }
                try
                {
                    using (DataTable wrk_Kal_pstgr = new DataTable()) //Kalendarz PSTRG
                    {
                        Log("START KALUPDATE " + (DateTime.Now - start));
                        using (NpgsqlConnection conA = new NpgsqlConnection(npA))
                        {
                            await conA.OpenAsync();
                            using (DataTable dbSchema = conA.GetSchema("Tables", new string[] { "zakupy", "public", "work_cal", null }))
                            {
                                // Czy tabela Demands Istnieje - jeśli nie to utwórz
                                if (dbSchema.Rows.Count == 0)
                                {
                                    using (NpgsqlCommand cmd = new NpgsqlCommand("CREATE TABLE work_cal (calendar_id varchar(15),counter integer,work_day date,day_type varchar(15),working_time double precision,working_periods integer,objid varchar(20) NOT NULL UNIQUE PRIMARY KEY,objversion varchar(14))", conA))
                                    {
                                        cmd.ExecuteNonQuery();
                                    }

                                }
                                using (NpgsqlCommand pot = new NpgsqlCommand("Select * from work_cal order by counter", conA))
                                {
                                    using (NpgsqlDataReader po = pot.ExecuteReader())
                                    {
                                        using (DataTable sch = po.GetSchemaTable())
                                        {
                                            foreach (DataRow a in sch.Rows)
                                            {
                                                wrk_Kal_pstgr.Columns.Add(a["ColumnName"].ToString(), System.Type.GetType(a["DataType"].ToString()));
                                                wrk_Kal_pstgr.Columns[a["ColumnName"].ToString()].AllowDBNull = true;
                                            }
                                        }
                                        wrk_Kal_pstgr.Load(po);
                                        Log("Start KAL " + (DateTime.Now - start));
                                    }
                                }
                            }
                            using (DataTable wrk_Kal = wrk_Kal_pstgr.Clone()) //Kalendarz z IFS
                            {
                                using (OracleConnection conO = new OracleConnection("Password = pass;User ID = user; Data Source = prod8"))
                                {
                                    await conO.OpenAsync();
                                    using (OracleCommand kal = new OracleCommand("" +
                                        "SELECT calendar_id,counter,to_date(work_day) work_day,day_type,working_time,working_periods,objid,objversion " +
                                        "FROM ifsapp.work_time_counter " +
                                        "WHERE CALENDAR_ID='SITS' ", conO))
                                    {
                                        //kal.FetchSize = kal.FetchSize * 128;
                                        using (OracleDataReader re = kal.ExecuteReader())
                                        {
                                            wrk_Kal.Load(re);
                                            Log("READY KAL " + (DateTime.Now - start));
                                        }
                                    }
                                }
                                using (NpgsqlTransaction TR_cal = conA.BeginTransaction())
                                {
                                    if (wrk_Kal_pstgr.Rows.Count > 0)
                                    {
                                        var data1 = wrk_Kal_pstgr.AsEnumerable().Except(wrk_Kal.AsEnumerable(), DataRowComparer.Default);
                                        using (DataTable TMP_kal_DEL = data1.Any() ? data1.CopyToDataTable() : new DataTable())
                                        {
                                            if (TMP_kal_DEL.Rows.Count > 0)
                                            {
                                                using (NpgsqlCommand cmd = new NpgsqlCommand("DELETE from work_cal where \"objid\"=@ID", conA))
                                                {
                                                    cmd.Parameters.Add("@ID", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                    cmd.Prepare();
                                                    foreach (DataRow row in TMP_kal_DEL.Rows)
                                                    {
                                                        cmd.Parameters[0].Value = row["objid"];
                                                        cmd.ExecuteNonQuery();
                                                    }
                                                    Log("END DELETE_OBJID: " + (DateTime.Now - start));
                                                }

                                            }
                                        }
                                    }
                                    var data = wrk_Kal.AsEnumerable().Except(wrk_Kal_pstgr.AsEnumerable(), DataRowComparer.Default);
                                    using (DataTable TMP_kal_NEW = data.Any() ? data.CopyToDataTable() : new DataTable())
                                    {
                                        if (TMP_kal_NEW.Rows.Count > 0)
                                        {
                                            using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                                                "INSERT INTO work_cal " +
                                                "(calendar_id,counter,work_day,day_type,working_time,working_periods,objid,objversion) " +
                                                "VALUES " +
                                                "(@calendar_id,@counter,@work_day,@day_type,@working_time,@working_periods,@objid,@objversion)", conA))
                                            {
                                                cmd.Parameters.Add("@calendar_id", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                cmd.Parameters.Add("@counter", NpgsqlTypes.NpgsqlDbType.Integer);
                                                cmd.Parameters.Add("@work_day", NpgsqlTypes.NpgsqlDbType.Date);
                                                cmd.Parameters.Add("@day_type", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                cmd.Parameters.Add("@working_time", NpgsqlTypes.NpgsqlDbType.Double);
                                                cmd.Parameters.Add("@working_periods", NpgsqlTypes.NpgsqlDbType.Integer);
                                                cmd.Parameters.Add("@objid", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                cmd.Parameters.Add("@objversion", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                cmd.Prepare();
                                                foreach (DataRow row in TMP_kal_NEW.Rows)
                                                {
                                                    cmd.Parameters[0].Value = row["calendar_id"];
                                                    cmd.Parameters[1].Value = row["counter"];
                                                    cmd.Parameters[2].Value = row["work_day"];
                                                    cmd.Parameters[3].Value = row["day_type"];
                                                    cmd.Parameters[4].Value = row["working_time"];
                                                    cmd.Parameters[5].Value = row["working_periods"];
                                                    cmd.Parameters[6].Value = row["objid"];
                                                    cmd.Parameters[7].Value = row["objversion"];
                                                    cmd.ExecuteNonQuery();
                                                }
                                                Log("END INSERT kal: " + (DateTime.Now - start));
                                            }
                                        }
                                    }
                                    TR_cal.Commit();
                                }
                            }
                            Log("READY KALUPDate " + (DateTime.Now - start));
                        }
                    }
                    using (NpgsqlConnection conA = new NpgsqlConnection(npC))
                    {
                        await conA.OpenAsync();
                        using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                            "UPDATE public.datatbles " +
                            "SET last_modify=current_timestamp, in_progress=false,updt_errors=false " +
                            "WHERE table_name='work_cal'", conA))
                        {
                            cmd.ExecuteNonQuery();
                        }
                    }
                    int w = 0;
                    int endcal = 0;
                    Parallel.Invoke(srv_op, async () => w = await Refr_wrkc(), async () => endcal = await Capacity());
                    GC.Collect();
                    if (w != 0) { Log("Błąd w WRKC"); }
                    return 0;
                }
                catch (Exception e)
                {
                    using (NpgsqlConnection conA = new NpgsqlConnection(npC))
                    {
                        await conA.OpenAsync();
                        using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                            "UPDATE public.datatbles " +
                            "SET in_progress=false,updt_errors=true " +
                            "WHERE table_name='work_cal'", conA))
                        {
                            cmd.ExecuteNonQuery();
                        }
                    }
                    Log("Błąd obliczeń przy imporcie kalendarza IFS:" + e);
                    return 1;
                }
            }
            catch (Exception e)
            {
                Log("Błąd Połączenia z POSTEGRESQL:" + e);
                return 1;
            }
        }
        private async Task<int> Get_ACCDEM()
        {
            try
            {
                Log("START ACC " + (DateTime.Now - start));
                // Utwórz połączenie z główną bazą

                using (NpgsqlConnection conA = new NpgsqlConnection(npA))
                {
                    await conA.OpenAsync();
                    using (NpgsqlCommand pot = new NpgsqlCommand("" +
                        "delete " +
                        "from public.demands " +
                        "where part_no||'_'||work_day in (select part_no||'_'||work_day id from demands " +
                        "group by part_no,work_day having count(part_no)>1)", conA))
                    {
                        pot.ExecuteNonQuery();
                    }
                    using (DataTable dbSchema = conA.GetSchema("Tables", new string[] { "zakupy", "public", "demands", null }))
                    {
                        // Czy tabela Demands Istnieje - jeśli nie to utwórz
                        if (dbSchema.Rows.Count == 0)
                        {
                            using (NpgsqlCommand cmd = new NpgsqlCommand("CREATE TABLE DEMANDS (PART_NO varchar(15), WORK_DAY date,EXPECTED_LEADTIME smallint,PURCH_QTY double precision,QTY_DEMAND double precision,TYPE_DMD smallint,BALANCE double precision,BAL_STOCK double precision,KOOR varchar(8) ,TYPE varchar(10) ,DAT_SHORTAGE timestamp without time zone ,ID UUID NOT NULL UNIQUE PRIMARY KEY,CHK_SUM double precision,objversion timestamp without time zone NOT NULL,CHKSUM Integer)", conA))
                            {
                                cmd.ExecuteNonQuery();
                            }
                            using (NpgsqlCommand cmd = new NpgsqlCommand("CREATE INDEX PART_NO ON DEMANDS (PART_NO ASC )", conA))
                            {
                                cmd.ExecuteNonQuery();
                            }
                        }
                    }
                    using (DataTable dbSchema = conA.GetSchema("Tables", new string[] { "zakupy", "public", "potw", null }))
                    {
                        // Czy tabela Potw Istnieje - jeśli nie to utwórz
                        if (dbSchema.Rows.Count == 0)
                        {
                            using (NpgsqlCommand cmd = new NpgsqlCommand("CREATE TABLE POTW (INDEKS varchar(15) ,DOST_ILOSC double precision,DATA_DOST date,SUM_DOST double precision,RODZAJ_POTW varchar(50),TERMIN_WAZN date,KOOR varchar(8) ,DATE_CREATED timestamp without time zone,ID UUID NOT NULL UNIQUE PRIMARY KEY)", conA))
                            {
                                cmd.ExecuteNonQuery();
                            }
                        }
                    }
                    using (NpgsqlCommand pot = new NpgsqlCommand("Select * from potw", conA))
                    {
                        using (NpgsqlDataReader po = pot.ExecuteReader())
                        {
                            POTW.Load(po);
                            POTW.DefaultView.Sort = "INDEKS ASC,DATA_DOST ASC";
                        }
                    }
                    using (NpgsqlCommand pot = new NpgsqlCommand("Select * from demands order by part_no,work_day", conA))
                    {
                        using (NpgsqlDataReader po = pot.ExecuteReader())
                        {
                            using (DataTable sch = po.GetSchemaTable())
                            {
                                foreach (DataRow a in sch.Rows)
                                {
                                    DEMANDS.Columns.Add(a["ColumnName"].ToString(), System.Type.GetType(a["DataType"].ToString()));
                                    DEMANDS.Columns[a["ColumnName"].ToString()].AllowDBNull = true;
                                }
                            }
                            DEMANDS.Load(po);
                            DEMANDS.DefaultView.Sort = "PART_no asc,work_day asc";
                        }
                    }
                    TASK_DEMANDS = DEMANDS.Clone();
                    conA.Close();
                }
                TASK_DEMANDS.Columns.Add("SRC_ID", System.Type.GetType("System.Guid"));
                TASK_DEMANDS.Columns.Add("Czynnosc");
                TASK_DEMANDS.Columns.Add("STATUS");
                Log("READY ACC " + (DateTime.Now - start));
                GC.Collect();
                return 0;
            }
            catch (Exception e)
            {
                Log("Błąd obliczeń przy pobraniu danych z Postgrsql dotyczących bilansów indeksów (tabela Demands):" + e);
                return 1;
            }
        }
        private async Task<int> Get_STmagO()
        {
            try
            {
                int endACC = await Get_ACCDEM();
                if (endACC == 1) { Log("Błąd wewnętrznej bazy danych"); }
                using (NpgsqlConnection conA = new NpgsqlConnection(npC))
                {
                    await conA.OpenAsync();
                    using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                        "UPDATE public.datatbles " +
                        "SET start_update=current_timestamp, in_progress=true " +
                        "WHERE table_name='mag'", conA))
                    {
                        cmd.ExecuteNonQuery();
                    }
                    conA.Close();
                }
                Log("START MAG " + (DateTime.Now - start));
                // Utwórz połączenie z ORACLE
                using (OracleConnection conO = new OracleConnection("Password = pass;User ID = user; Data Source = prod8"))
                {
                    await conO.OpenAsync();
                    using (OracleCommand mag = new OracleCommand("" +
                        "SELECT a.part_no Indeks,nvl(ifsapp.inventory_part_api.Get_Description('ST',a.part_no),' ') Opis,nvl(a.colection,' ') Kolekcja," +
                            "nvl(a.mag,0) Mag,PLANNER_BUYER,nvl(PART_PRODUCT_CODE,' ') Rodzaj,EXPECTED_LEADTIME Czas_dostawy," +
                            "ifsapp.work_time_calendar_api.Get_End_Date(ifsapp.site_api.Get_Manuf_Calendar_Id('ST'),SYSDATE,EXPECTED_LEADTIME) Data_gwarancji," +
                            "ifsapp.Inventory_Part_API.Get_Weight_Net('ST',PART_NO) Weight_Net,round(ifsapp.Inventory_Part_API.Get_Volume_Net('ST',PART_NO),2) Volume_Net," +
                            "round(ifsapp.inventory_part_unit_cost_api.Get_Inventory_Value_By_Config('ST',PART_NO,'*'),2) Inventory_Value," +
                            "NOTE_ID " +
                          "FROM " +
                             "(SELECT part_no,PLANNER_BUYER,ifsapp.inventory_part_in_stock_api. Get_Plannable_Qty_Onhand ('ST',part_no,'*') Mag,TYPE_DESIGNATION colection," +
                             "PART_PRODUCT_CODE,EXPECTED_LEADTIME,NOTE_ID " +
                                "FROM ifsapp.inventory_part_pub " +
                             "WHERE substr(part_no,1,1) in ('5','6') and TYPE_CODE_DB='4' ) a", conO))
                    {
                        //mag.FetchSize = mag.FetchSize * 256;
                        using (OracleDataReader re = mag.ExecuteReader())
                        {
                            using (DataTable mg = new DataTable())
                            {
                                mg.Load(re);
                                StMag = mg.Copy();
                                StMag.Columns.Remove("WEIGHT_NET");
                                StMag.Columns.Remove("VOLUME_NET");
                                StMag.Columns.Remove("INVENTORY_VALUE");
                                StMag.Columns.Remove("NOTE_ID");
                                StMag.DefaultView.Sort = "Indeks ASC";
                                mg.Columns.Remove("DATA_GWARANCJI");
                                mg.DefaultView.Sort = "note_id ASC";
                                using (NpgsqlConnection conA = new NpgsqlConnection(npA))
                                {
                                    await conA.OpenAsync();
                                    using (NpgsqlCommand pot = new NpgsqlCommand("Select * from mag order by note_id", conA))
                                    {
                                        using (NpgsqlDataReader po = pot.ExecuteReader())
                                        {
                                            using (DataTable mgpstg = new DataTable())
                                            {
                                                using (DataTable sch = po.GetSchemaTable())
                                                {
                                                    foreach (DataRow a in sch.Rows)
                                                    {
                                                        mgpstg.Columns.Add(a["ColumnName"].ToString(), System.Type.GetType(a["DataType"].ToString()));
                                                        mgpstg.Columns[a["ColumnName"].ToString()].AllowDBNull = true;
                                                    }
                                                }
                                                mgpstg.Load(po);
                                                using (DataTable tmp_mag = mgpstg.Clone())
                                                {
                                                    tmp_mag.Columns.Add("stat");
                                                    int maxrek = mgpstg.Rows.Count;
                                                    int ind = 0;
                                                    int counter = -1;
                                                    int max = mg.Rows.Count;
                                                    try
                                                    {
                                                        foreach (DataRowView rek in mg.DefaultView)
                                                        {
                                                            if (counter < max) { counter++; }
                                                            if (maxrek > ind)
                                                            {
                                                                while (Convert.ToDouble(rek["note_id"]) > Convert.ToDouble(mgpstg.DefaultView[ind].Row["note_id"]))
                                                                {
                                                                    //Debug.Print("DEL " + mgpstg.DefaultView[ind].Row[0]);
                                                                    DataRow rw = tmp_mag.NewRow();
                                                                    for (int i = 0; i < tmp_mag.Columns.Count - 1; i++)
                                                                    {
                                                                        rw[i] = mgpstg.DefaultView[ind].Row[i] ?? DBNull.Value;
                                                                    }
                                                                    rw[tmp_mag.Columns.Count - 1] = "DEL";
                                                                    tmp_mag.Rows.Add(rw);
                                                                    ind++;
                                                                    if (maxrek <= ind) { break; }
                                                                }
                                                                if (maxrek > ind)
                                                                {
                                                                    if (Convert.ToDouble(rek["note_id"]) == Convert.ToDouble(mgpstg.DefaultView[ind].Row["note_id"]))
                                                                    {
                                                                        //Debug.Print("Compare " + rek["note_id"]);
                                                                        bool chk = false;
                                                                        for (int i = 0; i < tmp_mag.Columns.Count - 2; i++)
                                                                        {
                                                                            bool db_or = false;
                                                                            bool db_pst = false;
                                                                            if (mgpstg.DefaultView[ind].Row[i] == DBNull.Value || mgpstg.DefaultView[ind].Row[i] == null) { db_pst = true; }
                                                                            if (rek[i] == DBNull.Value || rek[i] == null) { db_or = true; }
                                                                            if ((i == 3 || i > 5) && !db_or && !db_pst)
                                                                            {
                                                                                if (Convert.ToDouble(rek[i]) != Convert.ToDouble(mgpstg.DefaultView[ind].Row[i]))
                                                                                {
                                                                                    chk = true;
                                                                                }
                                                                            }
                                                                            else if (db_or || db_pst)
                                                                            {
                                                                                if (db_or != db_pst)
                                                                                {
                                                                                    chk = true;
                                                                                }
                                                                            }
                                                                            else
                                                                            {
                                                                                if (rek[i].ToString() != mgpstg.DefaultView[ind].Row[i].ToString())
                                                                                {
                                                                                    chk = true;
                                                                                }

                                                                            }
                                                                        }
                                                                        if (chk)
                                                                        {
                                                                            //Debug.Print("MOD " + rek["note_id"]);

                                                                            DataRow rw = tmp_mag.NewRow();
                                                                            for (int i = 0; i < tmp_mag.Columns.Count - 1; i++)
                                                                            {
                                                                                rw[i] = rek[i] ?? DBNull.Value;
                                                                            }
                                                                            rw[tmp_mag.Columns.Count - 1] = "MOD";
                                                                            tmp_mag.Rows.Add(rw);
                                                                        }
                                                                        ind++;
                                                                    }
                                                                    else
                                                                    {
                                                                        //Log("ADD " + rek["note_id"]);
                                                                        DataRow rw = tmp_mag.NewRow();
                                                                        for (int i = 0; i < tmp_mag.Columns.Count - 1; i++)
                                                                        {
                                                                            rw[i] = rek[i];
                                                                        }
                                                                        rw[tmp_mag.Columns.Count - 1] = "ADD";
                                                                        tmp_mag.Rows.Add(rw);
                                                                    }
                                                                }
                                                                else
                                                                {
                                                                    DataRow rw = tmp_mag.NewRow();
                                                                    for (int i = 0; i < tmp_mag.Columns.Count - 1; i++)
                                                                    {
                                                                        rw[i] = rek[i] ?? DBNull.Value;
                                                                    }
                                                                    rw[tmp_mag.Columns.Count - 1] = "ADD";
                                                                    tmp_mag.Rows.Add(rw);
                                                                }
                                                                if (counter >= max)
                                                                {
                                                                    while (maxrek > ind)
                                                                    {
                                                                        // Log("DEL " + mgpstg.DefaultView[ind].Row[0]);
                                                                        DataRow rw = tmp_mag.NewRow();
                                                                        for (int i = 0; i < tmp_mag.Columns.Count - 1; i++)
                                                                        {
                                                                            rw[i] = mgpstg.DefaultView[ind].Row[i] ?? DBNull.Value;
                                                                        }
                                                                        rw[tmp_mag.Columns.Count - 1] = "DEL";
                                                                        tmp_mag.Rows.Add(rw);
                                                                        ind++;
                                                                        if (maxrek <= ind) { break; }
                                                                    }
                                                                }
                                                            }
                                                            else
                                                            {
                                                                //Debug.Print("ADD " + rek["note_id"]);
                                                                DataRow rw = tmp_mag.NewRow();
                                                                for (int i = 0; i < tmp_mag.Columns.Count - 1; i++)
                                                                {
                                                                    rw[i] = rek[i] ?? DBNull.Value;
                                                                }
                                                                rw[tmp_mag.Columns.Count - 1] = "ADD";
                                                                tmp_mag.Rows.Add(rw);
                                                            }
                                                        }
                                                    }
                                                    catch (Exception e)
                                                    {
                                                        using (NpgsqlConnection conB = new NpgsqlConnection(npC))
                                                        {
                                                            await conB.OpenAsync();
                                                            using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                                                                "UPDATE public.datatbles " +
                                                                "SET in_progress=false,updt_errors=true " +
                                                                "WHERE table_name='mag'", conB))
                                                            {
                                                                cmd.ExecuteNonQuery();
                                                            }
                                                            conB.Close();
                                                        }
                                                        Log("Błąd aktualizacji stanów magazynowych:" + e);
                                                        return 1;
                                                    }
                                                    //Start update
                                                    using (NpgsqlTransaction TR_mag = conA.BeginTransaction())
                                                    {
                                                        DataRow[] rwA = tmp_mag.Select("stat='MOD'");
                                                        Log("RECORDS invent mod: " + rwA.Length);
                                                        if (rwA.Length > 0)
                                                        {
                                                            using (NpgsqlCommand cmd2 = new NpgsqlCommand("" +
                                                                "UPDATE public.mag " +
                                                                "SET indeks=@indeks, opis=@opis, kolekcja=@kolekcja, mag=@mag, planner_buyer=@planner_buyer," +
                                                                " rodzaj=@rodzaj, czas_dostawy=@czas_dostawy, \"WEIGHT_NET\"=@weight, \"VOLUME_NET\"=@volume, " +
                                                                "\"INVENTORY_VALUE\"=@value  where \"note_id\"=@note_id", conA))
                                                            {
                                                                cmd2.Parameters.Add("indeks", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                                cmd2.Parameters.Add("opis", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                                cmd2.Parameters.Add("kolekcja", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                                cmd2.Parameters.Add("mag", NpgsqlTypes.NpgsqlDbType.Double);
                                                                cmd2.Parameters.Add("planner_buyer", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                                cmd2.Parameters.Add("rodzaj", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                                cmd2.Parameters.Add("czas_dostawy", NpgsqlTypes.NpgsqlDbType.Integer);
                                                                cmd2.Parameters.Add("weight", NpgsqlTypes.NpgsqlDbType.Double);
                                                                cmd2.Parameters.Add("volume", NpgsqlTypes.NpgsqlDbType.Double);
                                                                cmd2.Parameters.Add("value", NpgsqlTypes.NpgsqlDbType.Double);
                                                                cmd2.Parameters.Add("note_id", NpgsqlTypes.NpgsqlDbType.Integer);
                                                                cmd2.Prepare();
                                                                foreach (DataRow rA in rwA)
                                                                {
                                                                    for (int i = 0; i < tmp_mag.Columns.Count - 1; i++)
                                                                    {
                                                                        cmd2.Parameters[i].Value = rA[i] ?? DBNull.Value;
                                                                    }
                                                                    cmd2.ExecuteNonQuery();
                                                                }
                                                                Log("END MODIFY mag for part:" + (DateTime.Now - start));
                                                            }
                                                        }
                                                        // DODAJ dane
                                                        rwA = tmp_mag.Select("stat='ADD'");
                                                        Log("RECORDS mag add: " + rwA.Length);
                                                        if (rwA.Length > 0)
                                                        {
                                                            using (NpgsqlCommand cmd2 = new NpgsqlCommand("" +
                                                                "INSERT " +
                                                                "INTO public.mag " +
                                                                "(indeks, opis, kolekcja, mag, planner_buyer, rodzaj, czas_dostawy, \"WEIGHT_NET\", \"VOLUME_NET\"," +
                                                                    " \"INVENTORY_VALUE\", note_id) " +
                                                                "VALUES " +
                                                                    "(@indeks, @opis, @kolekcja, @mag, @planner_buyer, @rodzaj, @czas_dostawy, @weight, @volume, @value, @note_id);", conA))
                                                            {
                                                                cmd2.Parameters.Add("indeks", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                                cmd2.Parameters.Add("opis", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                                cmd2.Parameters.Add("kolekcja", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                                cmd2.Parameters.Add("mag", NpgsqlTypes.NpgsqlDbType.Double);
                                                                cmd2.Parameters.Add("planner_buyer", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                                cmd2.Parameters.Add("rodzaj", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                                cmd2.Parameters.Add("czas_dostawy", NpgsqlTypes.NpgsqlDbType.Integer);
                                                                cmd2.Parameters.Add("weight", NpgsqlTypes.NpgsqlDbType.Double);
                                                                cmd2.Parameters.Add("volume", NpgsqlTypes.NpgsqlDbType.Double);
                                                                cmd2.Parameters.Add("value", NpgsqlTypes.NpgsqlDbType.Double);
                                                                cmd2.Parameters.Add("note_id", NpgsqlTypes.NpgsqlDbType.Integer);
                                                                cmd2.Prepare();
                                                                foreach (DataRow rA in rwA)
                                                                {
                                                                    for (int i = 0; i < tmp_mag.Columns.Count - 1; i++)
                                                                    {
                                                                        cmd2.Parameters[i].Value = rA[i] ?? DBNull.Value;
                                                                    }
                                                                    cmd2.ExecuteNonQuery();
                                                                }
                                                                Log("END ADD mag for part:" + (DateTime.Now - start));
                                                            }
                                                        }
                                                        rwA = tmp_mag.Select("stat='DEL'");
                                                        if (rwA.Length > 0)
                                                        {
                                                            using (NpgsqlCommand cmd2 = new NpgsqlCommand("delete from mag where \"note_id\"=@note_id", conA))
                                                            {
                                                                cmd2.Parameters.Add("@note_id", NpgsqlTypes.NpgsqlDbType.Integer);
                                                                cmd2.Prepare();
                                                                foreach (DataRow rA in rwA)
                                                                {
                                                                    cmd2.Parameters[0].Value = rA["note_id"];
                                                                    cmd2.ExecuteNonQuery();
                                                                }
                                                                Log("ERASE dont match mag for part" + (DateTime.Now - start));
                                                            }
                                                        }
                                                        TR_mag.Commit();
                                                    }
                                                }
                                            }
                                        }
                                    }
                                    conA.Close();
                                }
                            }
                        }
                    }
                    conO.Close();
                }
                Log("READY MAG " + (DateTime.Now - start));
                int wyn = await Get_DEMANDS();
                GC.Collect();
                if (wyn == 0)
                {
                    using (NpgsqlConnection conA = new NpgsqlConnection(npC))
                    {
                        await conA.OpenAsync();
                        using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                            "UPDATE public.datatbles " +
                            "SET last_modify=current_timestamp, in_progress=false,updt_errors=false " +
                            "WHERE table_name='mag'", conA))
                        {
                            cmd.ExecuteNonQuery();
                        }
                        conA.Close();
                    }
                    return 0;
                }
                else
                {
                    using (NpgsqlConnection conA = new NpgsqlConnection(npC))
                    {
                        await conA.OpenAsync();
                        using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                            "UPDATE public.datatbles " +
                            "SET in_progress=false,updt_errors=true " +
                            "WHERE table_name='mag'", conA))
                        {
                            cmd.ExecuteNonQuery();
                        }
                        conA.Close();
                    }
                    return 1;
                }
            }
            catch (Exception e)
            {
                using (NpgsqlConnection conA = new NpgsqlConnection(npC))
                {
                    await conA.OpenAsync();
                    using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                        "UPDATE public.datatbles " +
                        "SET in_progress=false,updt_errors=true " +
                        "WHERE table_name='mag'", conA))
                    {
                        cmd.ExecuteNonQuery();
                    }
                    conA.Close();
                }
                Log("Błąd aktualizacji stanów magazynowych w POSTGRSQL:" + e);
                return 1;
            }
        }
        private async Task<int> Get_DMNDO()
        {
            try
            {
                Log("START DMND " + (DateTime.Now - start));
                // Utwórz połączenie z ORACLE

                using (OracleConnection conO = new OracleConnection("Password = pass;User ID = user; Data Source = prod8"))
                {
                    await conO.OpenAsync();
                    using (OracleCommand mag = new OracleCommand("" +
                        "SELECT PART_NO,To_Date(DATE_REQUIRED) DATE_REQUIRED,round(Sum(QTY_SUPPLY),10) QTY_SUPPLY,round(Sum(QTY_DEMAND),10) QTY_DEMAND," +
                            "Nvl(round(Sum(QTY_DEMAND_ZAM),10),0) DEMAND_ZAM,Nvl(round(Sum(QTY_DEMAND_DOP),10),0) QTY_DEMAND_DOP," +
                            "ifsapp.work_time_calendar_api.Get_Next_Work_Day(ifsapp.site_api.Get_Manuf_Calendar_Id('ST'),To_Date(DATE_REQUIRED)) NextDay," +
                            "Sum(chksum) chk_sum " +
                        "FROM " +
                            "( SELECT PART_NO, DATE_REQUIRED, 0 QTY_SUPPLY, QTY_DEMAND, 0 QTY_DEMAND_DOP, 0 QTY_DEMAND_ZAM, owa_opt_lock.checksum(a.ROWID) chksum " +
                                "FROM ifsapp.shop_material_alloc_demand a " +
                                "WHERE SubStr(part_no, 1, 1) IN('5', '6')   " +
                            "UNION ALL  " +
                            "SELECT PART_NO, DATE_REQUIRED, 0 QTY_SUPPLY, QTY_DEMAND, QTY_DEMAND QTY_DEMAND_DOP, 0 QTY_DEMAND_ZAM," +
                                "owa_opt_lock.checksum(order_no||QTY_DEMAND||DATE_REQUIRED||ORDER_NO||LINE_NO||INFO) chksum " +
                                "FROM ifsapp.dop_order_demand_ext " +
                                "WHERE SubStr(part_no, 1, 1) IN('5', '6') " +
                            "UNION ALL " +
                            "SELECT PART_NO, DATE_REQUIRED, 0 QTY_SUPPLY, QTY_DEMAND, 0 QTY_DEMAND_DOP, QTY_DEMAND QTY_DEMAND_ZAM," +
                                "owa_opt_lock.checksum(ROW_ID||QTY_DEMAND||DATE_REQUIRED||QTY_PEGGED||QTY_RESERVED) chksum " +
                                "FROM ifsapp.customer_order_line_demand_oe " +
                                "WHERE SubStr(part_no, 1, 1) IN('5', '6') " +
                            "UNION ALL " +
                            "SELECT PART_NO, DATE_REQUIRED, 0 QTY_SUPPLY, QTY_DEMAND, 0 QTY_DEMAND_DOP, 0 QTY_DEMAND_ZAM," +
                                "owa_opt_lock.checksum(ROWID||QTY_DEMAND||DATE_REQUIRED||STATUS_CODE) chksum " +
                                "FROM ifsapp.material_requis_line_demand_oe " +
                                "WHERE SubStr(part_no, 1, 1) IN('5', '6') " +
                            "UNION ALL  " +
                            "SELECT PART_NO,ifsapp.work_time_calendar_api.Get_Closest_Work_Day(ifsapp.site_api.Get_Manuf_Calendar_Id('ST'),SYSDATE) DATE_REQUIRED," +
                                "QTY_SUPPLY,0 QTY_DEMAND,0 QTY_DEMAND_DOP,0 QTY_DEMAND_ZAM, owa_opt_lock.checksum(ROWID||QTY_SUPPLY||DATE_REQUIRED||STATUS_CODE) chksum " +
                                "FROM ifsapp.ARRIVED_PUR_ORDER_EXT  " +
                                "where SubStr(part_no, 1, 1) IN('5', '6')  " +
                            "UNION ALL  " +
                            "SELECT PART_NO, DATE_REQUIRED, QTY_SUPPLY, 0 QTY_DEMAND, 0 QTY_DEMAND_DOP, 0 QTY_DEMAND_ZAM," +
                                "owa_opt_lock.checksum(ROWID||QTY_SUPPLY||DATE_REQUIRED) chksum " +
                                "FROM ifsapp.purchase_order_line_supply  " +
                                "WHERE SubStr(part_no, 1, 1) IN('5', '6') " +
                            "UNION ALL " +
                            "SELECT PART_NO, DATE_REQUIRED, QTY_SUPPLY, QTY_DEMAND,0 QTY_DEMAND_DOP,0 QTY_DEMAND_ZAM," +
                                "owa_opt_lock.checksum(ROWID||QTY_SUPPLY||DATE_REQUIRED) chksum " +
                                "FROM ifsapp.purchase_order_res_ext  " +
                                "WHERE SubStr(part_no, 1, 1) IN('5', '6') " +
                             "UNION ALL " +
                             "SELECT PART_NO, DATE_REQUIRED, QTY_SUPPLY, QTY_DEMAND,0 QTY_DEMAND_DOP,0  QTY_DEMAND_ZAM," +
                                "owa_opt_lock.checksum(ROWID||QTY_SUPPLY||DATE_REQUIRED) chksum " +
                                "FROM ifsapp.PUR_ORD_CHARGED_COMP_DEMAND_XT  " +
                                "WHERE SubStr(part_no, 1, 1) IN('5', '6'))" +
                       "GROUP BY PART_NO, To_Date(DATE_REQUIRED) ", conO))
                    {

                        using (OracleDataReader re = mag.ExecuteReader())
                        {
                            DMND_ORA.Load(re);
                            DMND_ORA.DefaultView.Sort = "PART_NO ASC,DATE_REQUIRED ASC";
                        }
                    }
                    conO.Close();
                }
                Log("REaDY DMND " + (DateTime.Now - start));
                GC.Collect();
                return 0;
            }
            catch (Exception e)
            {
                Log("Błąd pobrania planów dostaw/potrzeb z IFS:" + e);
                return 1;
            }
        }
        private async Task<int> Get_DEMANDS()
        {
            try
            {
                using (NpgsqlConnection conA = new NpgsqlConnection(npA))
                {
                    await conA.OpenAsync();
                    using (DataTable dbSchema = conA.GetSchema("Tables", new string[] { "zakupy", "public", "data", null }))
                    {
                        // Sprawdź czy tabela data istnieje / jak nie to utówrz
                        if (dbSchema.Rows.Count == 0)
                        {
                            string Tmp_txt = "";
                            foreach (DataColumn col in StMag.Columns)
                            {
                                string typ = GetOleDbType(col.DataType).ToString();
                                if (typ == "Double") { typ = "double precision"; }
                                Tmp_txt = Tmp_txt + col.ColumnName + " " + typ + " " + GE_LEN_DATA(col.ColumnName) + ",";
                            }
                            Tmp_txt = Tmp_txt + "Przyczyna varchar(25) ,Data_Dost date,Wlk_Dost double precision,Bilans double precision,Data_Braku date,Bil_dost_Dzień double precision,TYP_zdarzenia varchar(35),Widoczny_od_Dnia timestamp,Sum_Dost double precision,Sum_Potrz double precision,Sum_Dost_opóźnion double precision,Sum_Potrz_opóźnion double precision,Status_Informacji varchar(35),REFR_DATE timestamp,ID UUID NOT NULL UNIQUE PRIMARY KEY,chk double precision";
                            using (NpgsqlCommand cmd = new NpgsqlCommand("CREATE TABLE DATA (@Tmp_txt)", conA))
                            {
                                cmd.Parameters.Add("Tmp_txt", NpgsqlTypes.NpgsqlDbType.Varchar).Value = Tmp_txt;
                                cmd.Prepare();
                                cmd.ExecuteNonQuery();
                            }
                            //Utwórz Indeks w tabeli
                            using (NpgsqlCommand cmd = new NpgsqlCommand("CREATE INDEX Indeks ON DATA (Indeks ASC )", conA))
                            {
                                cmd.ExecuteNonQuery();
                            }
                        }
                    }
                    Log("RUN_GET_DEMANDS" + (DateTime.Now - start));
                    using (NpgsqlCommand pot = new NpgsqlCommand("Select * from data order by indeks,data_dost", conA))
                    {
                        using (NpgsqlDataReader po = pot.ExecuteReader())
                        {
                            using (DataTable sch = po.GetSchemaTable())
                            {
                                foreach (DataRow a in sch.Rows)
                                {
                                    dta.Columns.Add(a["ColumnName"].ToString(), System.Type.GetType(a["DataType"].ToString()));
                                    dta.Columns[a["ColumnName"].ToString()].AllowDBNull = true;
                                }
                            }
                            dta.Load(po);
                            dta.DefaultView.Sort = "Indeks asc,DATA_DOST asc";
                        }
                    }
                    TASK_dat = dta.Clone();
                    conA.Close();
                }
                TASK_dat.Columns.Remove("ID");
                TASK_dat.Columns.Add("SRC_ID", System.Type.GetType("System.Guid"));
                TASK_dat.Columns.Add("Czynnosc");
                TASK_dat.Columns.Add("STATUS");
                TMPTASK_dat = TASK_dat.Clone();

                Log("END_GET_DEMANDS" + (DateTime.Now - start));
                GC.Collect();
                return 0;
            }
            catch
            {
                Log("ERROR_IN_GET_DEMANDS" + (DateTime.Now - start));
                return 1;
            }
        }
        private async Task<int> DO_JOB_DEM()
        {
            try
            {
                using (NpgsqlConnection conA = new NpgsqlConnection(npA))
                {
                    await conA.OpenAsync();
                    using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                        "UPDATE public.datatbles " +
                        "SET start_update=current_timestamp, in_progress=true " +
                        "WHERE table_name='demands'", conA))
                    {
                        cmd.ExecuteNonQuery();
                    }
                    conA.Close();
                }
                using (NpgsqlConnection conB = new NpgsqlConnection(npA))
                {
                    conB.Open();
                    using (NpgsqlTransaction TR_demands = conB.BeginTransaction())
                    {
                        Log("START DELETE_PART: " + (DateTime.Now - start));
                        DataRow[] rw = TASK_DEMANDS.Select("Czynnosc= 'DEL_PART_DEM' and Status='Zapl.'");
                        Log("RECORDS DELETE_PART: " + rw.Length);
                        if (rw.Length > 0)
                        {
                            using (NpgsqlCommand cmd = new NpgsqlCommand("DELETE from DEMANDS where \"part_no\" = @PART_NO", conB))
                            {
                                cmd.Parameters.Add("PART_NO", NpgsqlTypes.NpgsqlDbType.Varchar);
                                cmd.Prepare();
                                foreach (DataRow row in rw)
                                {
                                    cmd.Parameters[0].Value = row["PART_NO"];
                                    cmd.ExecuteNonQuery();
                                }
                                rw = null;
                                Log("END DELETE_PART: " + (DateTime.Now - start));
                            }
                        }
                        Log("START DELETE_GIUD: " + (DateTime.Now - start));
                        rw = TASK_DEMANDS.Select("Czynnosc= 'DEL_ID_DEM' and Status='Zapl.'");
                        Log("RECORDS DELETE_GIUD: " + rw.Length);
                        if (rw.Length > 0)
                        {
                            using (NpgsqlCommand cmd = new NpgsqlCommand("DELETE from DEMANDS where \"id\"=@ID", conB))
                            {
                                cmd.Parameters.Add("@ID", NpgsqlTypes.NpgsqlDbType.Uuid);
                                cmd.Prepare();
                                foreach (DataRow row in rw)
                                {
                                    cmd.Parameters[0].Value = row["Src_ID"];
                                    cmd.ExecuteNonQuery();
                                }
                                rw = null;
                                Log("END DELETE_GIUD: " + (DateTime.Now - start));
                            }
                        }
                        Log("Start Modify: " + (DateTime.Now - start));
                        rw = TASK_DEMANDS.Select("Czynnosc= 'MoD_DEM' and Status='Zapl.'");
                        Log("RECORDS Modify: " + rw.Length);
                        if (rw.Length > 0)
                        {
                            using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                                "UPDATE DEMANDS " +
                                "SET EXPECTED_LEADTIME=@EXPECTED_LEADTIME,PURCH_QTY= @PURCH_QTY,QTY_DEMAND = @QTY_DEMAND, TYPE_DMD = @TYPE_DMD," +
                                    " BALANCE = @BALANCE,BAL_STOCK = @BAL_STOCK, KOOR = @KOOR, TYPE = @TYPE, DAT_SHORTAGE = @DAT_SHORTAGE,CHK_SUM = @SHK_SUM," +
                                    "objversion=@objversion,chksum=@CHKSUM where \"id\"=@ID", conB))
                            {
                                cmd.Parameters.Add("@EXPECTED_LEADTIME", NpgsqlTypes.NpgsqlDbType.Smallint);
                                cmd.Parameters.Add("@PURCH_QTY", NpgsqlTypes.NpgsqlDbType.Double);
                                cmd.Parameters.Add("@QTY_DEMAND", NpgsqlTypes.NpgsqlDbType.Double);
                                cmd.Parameters.Add("@TYPE_DMD", NpgsqlTypes.NpgsqlDbType.Smallint);
                                cmd.Parameters.Add("@BALANCE", NpgsqlTypes.NpgsqlDbType.Double);
                                cmd.Parameters.Add("@BAL_STOCK", NpgsqlTypes.NpgsqlDbType.Double);
                                cmd.Parameters.Add("@KOOR", NpgsqlTypes.NpgsqlDbType.Varchar);
                                cmd.Parameters.Add("@TYPE", NpgsqlTypes.NpgsqlDbType.Varchar);
                                cmd.Parameters.Add("@DAT_SHORTAGE", NpgsqlTypes.NpgsqlDbType.Timestamp);
                                cmd.Parameters.Add("@SHK_SUM", NpgsqlTypes.NpgsqlDbType.Double);
                                cmd.Parameters.Add("@ID", NpgsqlTypes.NpgsqlDbType.Uuid);
                                cmd.Parameters.Add("@objversion", NpgsqlTypes.NpgsqlDbType.Timestamp);
                                cmd.Parameters.Add("@CHKSUM", NpgsqlTypes.NpgsqlDbType.Integer);
                                cmd.Prepare();
                                foreach (DataRow row in rw)
                                {
                                    if (!row.IsNull("EXPECTED_LEADTIME"))
                                    {
                                        cmd.Parameters[0].Value = row["EXPECTED_LEADTIME"];
                                        cmd.Parameters[1].Value = row["PURCH_QTY"];
                                        cmd.Parameters[2].Value = row["QTY_DEMAND"];
                                        cmd.Parameters[3].Value = row["TYPE_DMD"];
                                        cmd.Parameters[4].Value = row["BALANCE"];
                                        cmd.Parameters[5].Value = row["BAL_STOCK"];
                                        cmd.Parameters[6].Value = row["KOOR"];
                                        cmd.Parameters[7].Value = row["TYPE"];
                                        cmd.Parameters[8].Value = row["DAT_SHORTAGE"] ?? DBNull.Value;
                                        cmd.Parameters[9].Value = row["chk_sum"];
                                        cmd.Parameters[10].Value = row["Src_ID"];
                                        cmd.Parameters[11].Value = row["objversion"];
                                        cmd.Parameters[12].Value = row["chksum"];
                                        cmd.ExecuteNonQuery();
                                    }
                                }
                                rw = null;
                                Log("END Modify: " + (DateTime.Now - start));
                            }
                        }

                        Log("START INSERT: " + (DateTime.Now - start));
                        rw = TASK_DEMANDS.Select("Czynnosc= 'ADD_NEW_DEM' and Status='Zapl.'");
                        Log("RECORDS INSERT: " + rw.Length);
                        if (rw.Length > 0)
                        {
                            using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                                "INSERT INTO DEMANDS " +
                                "(PART_NO,WORK_DAY,EXPECTED_LEADTIME,PURCH_QTY,QTY_DEMAND,TYPE_DMD,BALANCE,BAL_STOCK,KOOR,TYPE,DAT_SHORTAGE,ID,chk_sum,objversion,CHKSUM) " +
                                "VALUES" +
                                "(@PART_NO,@WORK_DAY,@EXPECTED_LEADTIME,@PURCH_QTY,@QTY_DEMAND,@TYPE_DMD,@BALANCE,@BAL_STOCK,@KOOR,@TYPE,@DAT_SHORTAGE,@ID,@chk_sum,@objversion,@CHKSUM)", conB))
                            {
                                cmd.Parameters.Add("@PART_NO", NpgsqlTypes.NpgsqlDbType.Varchar);
                                cmd.Parameters.Add("@WORK_DAY", NpgsqlTypes.NpgsqlDbType.Date);
                                cmd.Parameters.Add("@EXPECTED_LEADTIME", NpgsqlTypes.NpgsqlDbType.Smallint);
                                cmd.Parameters.Add("@PURCH_QTY", NpgsqlTypes.NpgsqlDbType.Double);
                                cmd.Parameters.Add("@QTY_DEMAND", NpgsqlTypes.NpgsqlDbType.Double);
                                cmd.Parameters.Add("@TYPE_DMD", NpgsqlTypes.NpgsqlDbType.Smallint);
                                cmd.Parameters.Add("@BALANCE", NpgsqlTypes.NpgsqlDbType.Double);
                                cmd.Parameters.Add("@BAL_STOCK", NpgsqlTypes.NpgsqlDbType.Double);
                                cmd.Parameters.Add("@KOOR", NpgsqlTypes.NpgsqlDbType.Varchar);
                                cmd.Parameters.Add("@TYPE", NpgsqlTypes.NpgsqlDbType.Varchar);
                                cmd.Parameters.Add("@DAT_SHORTAGE", NpgsqlTypes.NpgsqlDbType.Timestamp);
                                cmd.Parameters.Add("@ID", NpgsqlTypes.NpgsqlDbType.Uuid);
                                cmd.Parameters.Add("@chk_sum", NpgsqlTypes.NpgsqlDbType.Double);
                                cmd.Parameters.Add("@objversion", NpgsqlTypes.NpgsqlDbType.Timestamp);
                                cmd.Parameters.Add("@CHKSUM", NpgsqlTypes.NpgsqlDbType.Integer);
                                cmd.Prepare();
                                foreach (DataRow row in rw)
                                {
                                    cmd.Parameters[0].Value = row["PART_NO"];
                                    cmd.Parameters[1].Value = row["WORK_DAY"];
                                    cmd.Parameters[2].Value = row["EXPECTED_LEADTIME"];
                                    cmd.Parameters[3].Value = row["PURCH_QTY"];
                                    cmd.Parameters[4].Value = row["QTY_DEMAND"];
                                    cmd.Parameters[5].Value = row["TYPE_DMD"];
                                    cmd.Parameters[6].Value = row["BALANCE"];
                                    cmd.Parameters[7].Value = row["BAL_STOCK"];
                                    cmd.Parameters[8].Value = row["KOOR"];
                                    cmd.Parameters[9].Value = row["TYPE"];
                                    cmd.Parameters[10].Value = row["DAT_SHORTAGE"] ?? DBNull.Value;
                                    cmd.Parameters[11].Value = System.Guid.NewGuid();
                                    cmd.Parameters[12].Value = row["chk_sum"];
                                    cmd.Parameters[13].Value = row["objversion"];
                                    cmd.Parameters[14].Value = row["CHKSUM"];
                                    cmd.ExecuteNonQuery();
                                    row.Delete();
                                }
                                rw = null;
                                Log("END INSERT: " + (DateTime.Now - start));
                            }
                        }
                        using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                            "DELETE " +
                            "FROM public.potw " +
                            "WHERE id in " +
                                "(select a.id " +
                                     "from " +
                                     "((select * from potw where rodzaj_potw!='NIE ZAMAWIAM') a " +
                                        "left join " +
                                        "(select part_no,work_day,purch_qty from demands where purch_qty>0) b " +
                                        "on b.part_no=a.indeks and b.work_day=a.data_dost and b.purch_qty=a.dost_ilosc) " +
                                   "where b.part_no is null)", conB))
                        {
                            cmd.ExecuteNonQuery();
                        }
                        using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                            "DELETE " +
                            "FROM public.potw " +
                            "WHERE (termin_wazn<current_date or data_dost<current_date) and rodzaj_potw!='NIE ZAMAWIAM' ", conB))
                        {
                            cmd.ExecuteNonQuery();
                        }
                        using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                            "UPDATE public.datatbles " +
                            "SET last_modify=current_timestamp, in_progress=false,updt_errors=false " +
                            "WHERE table_name='demands'", conB))
                        {
                            cmd.ExecuteNonQuery();
                        }
                        TR_demands.Commit();
                    }
                    Log("END_UPDATE_DMND " + (DateTime.Now - start));
                    GC.Collect();
                    return 0;
                }
            }
            catch (Exception e)
            {
                using (NpgsqlConnection conA = new NpgsqlConnection(npA))
                {
                    await conA.OpenAsync();
                    using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                        "UPDATE public.datatbles " +
                        "SET in_progress=false,updt_errors=true " +
                        "WHERE table_name='demands'", conA))
                    {
                        cmd.ExecuteNonQuery();
                    }
                    conA.Close();
                }
                Log("Błąd modyfikacji tabeli źródłowej demands:" + e);
                return 1;
            }
        }
        private async Task<int> DO_JOB_dta()
        {
            try
            {
                using (NpgsqlConnection conA = new NpgsqlConnection(npA))
                {
                    await conA.OpenAsync();
                    using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                        "UPDATE public.datatbles " +
                        "SET start_update=current_timestamp, in_progress=true " +
                        "WHERE table_name='data'", conA))
                    {
                        cmd.ExecuteNonQuery();
                    }
                    conA.Close();
                }
                using (NpgsqlConnection conB = new NpgsqlConnection(npA))
                {
                    Log("START UPDATE_dta: " + (DateTime.Now - start));
                    conB.Open();
                    using (NpgsqlTransaction TR_data = conB.BeginTransaction())
                    {
                        Log("START DELETE_PART_dta: " + (DateTime.Now - start));
                        DataRow[] rw = TASK_dat.Select("Czynnosc= 'DEL_PART_DEM' and Status='Zapl.'");
                        Log("RECORDS DELETE_PART: " + rw.Length);
                        if (rw.Length > 0)
                        {
                            using (NpgsqlCommand cmd = new NpgsqlCommand("DELETE from data where \"indeks\" = @indeks", conB))
                            {
                                cmd.Parameters.Add("@indeks", NpgsqlTypes.NpgsqlDbType.Varchar);
                                cmd.Prepare();
                                foreach (DataRow row in rw)
                                {
                                    cmd.Parameters[0].Value = row["indeks"];
                                    cmd.ExecuteNonQuery();
                                }
                                rw = null;
                                Log("END DELETE_indeks_dta: " + (DateTime.Now - start));
                            }
                        }
                        Log("START DELETE_GIUD_dta: " + (DateTime.Now - start));
                        rw = TASK_dat.Select("Czynnosc= 'DEL_ID_DEM' and Status='Zapl.'");
                        Log("RECORDS DELETE_GIUD: " + rw.Length);
                        if (rw.Length > 0)
                        {
                            using (NpgsqlCommand cmd = new NpgsqlCommand("DELETE from data where \"id\"=@ID", conB))
                            {
                                cmd.Parameters.Add("@ID", NpgsqlTypes.NpgsqlDbType.Uuid);
                                cmd.Prepare();
                                foreach (DataRow row in rw)
                                {
                                    cmd.Parameters[0].Value = row["Src_ID"];
                                    cmd.ExecuteNonQuery();
                                }
                                rw = null;
                                Log("END DELETE_GIUD_data: " + (DateTime.Now - start));
                            }
                        }
                        Log("Start Modify_dta: " + (DateTime.Now - start));
                        rw = TASK_dat.Select("Czynnosc= 'MoD_DEM' and Status='Zapl.'");
                        Log("RECORDS Modify: " + rw.Length);
                        if (rw.Length > 0)
                        {
                            using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                                "UPDATE data " +
                                "SET OPIS=@OPIS,KOLEKCJA=@KOLEKCJA,MAG=@MAG,PLANNER_BUYER=@PLANNER_BUYER,RODZAJ=@RODZAJ,CZAS_DOSTAWY=@CZAS_DOSTAWY," +
                                     "DATA_GWARANCJI=@DATA_GWARANCJI,data_dost=@DATA_DOST,PRZYCZYNA=@PRZYCZYNA,WLK_DOST=@WLK_DOST,bilans=@bilans," +
                                     "bil_dost_dzień=@bil_dost_dzień,DATA_BRAKU=@DATA_BRAKU,typ_zdarzenia=@typ_zdarzenia,widoczny_od_dnia=@widoczny_od_dnia," +
                                     "sum_dost=@sum_dost,sum_potrz=@sum_potrz,sum_dost_opóźnion=@sum_dost_opóźnion,sum_potrz_opóźnion=@sum_potrz_opóźnion," +
                                     "Status_informacji=@Status_informacji,refr_date=@refr_date,chk=@chk " +
                                 "where \"id\"=@id", conB))
                            {
                                cmd.Parameters.Add("@OPIS", NpgsqlTypes.NpgsqlDbType.Varchar);
                                cmd.Parameters.Add("@KOLEKCJA", NpgsqlTypes.NpgsqlDbType.Varchar);
                                cmd.Parameters.Add("@MAG", NpgsqlTypes.NpgsqlDbType.Double);
                                cmd.Parameters.Add("@PLANNER_BUYER", NpgsqlTypes.NpgsqlDbType.Varchar);
                                cmd.Parameters.Add("@RODZAJ", NpgsqlTypes.NpgsqlDbType.Varchar);
                                cmd.Parameters.Add("@CZAS_DOSTAWY", NpgsqlTypes.NpgsqlDbType.Double);
                                cmd.Parameters.Add("@DATA_GWARANCJI", NpgsqlTypes.NpgsqlDbType.Date);
                                cmd.Parameters.Add("@DATA_DOST", NpgsqlTypes.NpgsqlDbType.Date);
                                cmd.Parameters.Add("@PRZYCZYNA", NpgsqlTypes.NpgsqlDbType.Integer);
                                cmd.Parameters.Add("@WLK_DOST", NpgsqlTypes.NpgsqlDbType.Double);
                                cmd.Parameters.Add("@bilans", NpgsqlTypes.NpgsqlDbType.Double);
                                cmd.Parameters.Add("@bil_dost_dzień", NpgsqlTypes.NpgsqlDbType.Double);
                                cmd.Parameters.Add("@DATA_BRAKU", NpgsqlTypes.NpgsqlDbType.Date);
                                cmd.Parameters.Add("@typ_zdarzenia", NpgsqlTypes.NpgsqlDbType.Varchar);
                                cmd.Parameters.Add("@widoczny_od_dnia", NpgsqlTypes.NpgsqlDbType.Timestamp);
                                cmd.Parameters.Add("@sum_dost", NpgsqlTypes.NpgsqlDbType.Double);
                                cmd.Parameters.Add("@sum_potrz", NpgsqlTypes.NpgsqlDbType.Double);
                                cmd.Parameters.Add("@sum_dost_opóźnion", NpgsqlTypes.NpgsqlDbType.Double);
                                cmd.Parameters.Add("@sum_potrz_opóźnion", NpgsqlTypes.NpgsqlDbType.Double);
                                cmd.Parameters.Add("@Status_informacji", NpgsqlTypes.NpgsqlDbType.Varchar);
                                cmd.Parameters.Add("@refr_date", NpgsqlTypes.NpgsqlDbType.Timestamp);
                                cmd.Parameters.Add("@chk", NpgsqlTypes.NpgsqlDbType.Double);
                                cmd.Parameters.Add("@id", NpgsqlTypes.NpgsqlDbType.Uuid);
                                cmd.Prepare();
                                foreach (DataRow row in rw)
                                {
                                    try
                                    {
                                        cmd.Parameters[0].Value = row["OPIS"];
                                        cmd.Parameters[1].Value = row["KOLEKCJA"];
                                        cmd.Parameters[2].Value = row["MAG"];
                                        cmd.Parameters[3].Value = row["PLANNER_BUYER"];
                                        cmd.Parameters[4].Value = row["RODZAJ"];
                                        cmd.Parameters[5].Value = row["CZAS_DOSTAWY"];
                                        cmd.Parameters[6].Value = row["DATA_GWARANCJI"] ?? DBNull.Value;
                                        cmd.Parameters[7].Value = row["DATA_DOST"] ?? DBNull.Value;
                                        cmd.Parameters[8].Value = row["PRZYCZYNA"];
                                        cmd.Parameters[9].Value = row["WLK_DOST"];
                                        cmd.Parameters[10].Value = row["bilans"];
                                        cmd.Parameters[11].Value = row["bil_dost_dzień"];
                                        cmd.Parameters[12].Value = row["DATA_BRAKU"] ?? DBNull.Value;
                                        cmd.Parameters[13].Value = row["typ_zdarzenia"];
                                        cmd.Parameters[14].Value = row["widoczny_od_dnia"] ?? DBNull.Value;
                                        cmd.Parameters[15].Value = row["sum_dost"];
                                        cmd.Parameters[16].Value = row["sum_potrz"];
                                        cmd.Parameters[17].Value = row["sum_dost_opóźnion"];
                                        cmd.Parameters[18].Value = row["sum_potrz_opóźnion"];
                                        cmd.Parameters[19].Value = row["Status_informacji"];
                                        cmd.Parameters[20].Value = row["refr_date"] ?? DBNull.Value;
                                        cmd.Parameters[21].Value = row["chk"];
                                        cmd.Parameters[22].Value = row["Src_id"];
                                        cmd.ExecuteNonQuery();
                                    }
                                    catch
                                    {
                                        Log("Modyfikacja tabeli Zakupy Błąd - coś nie tak z :" + row["Indeks"] + "_" + row["data_dost"]);
                                    }
                                }
                                rw = null;
                                Log("END Modify_dta: " + (DateTime.Now - start));
                            }
                        }
                        Log("START INSERT_dta: " + (DateTime.Now - start));
                        rw = TASK_dat.Select("Czynnosc= 'ADD_NEW_DEM' and Status='Zapl.'");
                        Log("RECORDS INSERT: " + rw.Length);
                        if (rw.Length > 0)
                        {
                            using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                                "INSERT INTO data " +
                                "(indeks, opis, kolekcja, mag, planner_buyer, rodzaj, czas_dostawy, data_gwarancji, data_dost, wlk_dost, bilans," +
                                    " data_braku, \"bil_dost_dzień\", typ_zdarzenia, widoczny_od_dnia, sum_dost, sum_potrz, \"sum_dost_opóźnion\"," +
                                    " \"sum_potrz_opóźnion\", status_informacji, refr_date, id, chk, przyczyna) " +
                                "VALUES " +
                                "(@indeks,@OPIS,@KOLEKCJA,@MAG,@PLANNER_BUYER,@RODZAJ,@CZAS_DOSTAWY,@DATA_GWARANCJI,@DATA_DOST,@WLK_DOST,@bilans," +
                                "@DATA_BRAKU,@bil_dost_dzień,@typ_zdarzenia,@widoczny_od_dnia,@sum_dost,@sum_potrz,@sum_dost_opóźnion,@sum_potrz_opóźnion," +
                                "@Status_informacji,@refr_date,@id,@chk,@PRZYCZYNA)", conB))
                            {
                                cmd.Parameters.Add("@indeks", NpgsqlTypes.NpgsqlDbType.Varchar);
                                cmd.Parameters.Add("@OPIS", NpgsqlTypes.NpgsqlDbType.Varchar);
                                cmd.Parameters.Add("@KOLEKCJA", NpgsqlTypes.NpgsqlDbType.Varchar);
                                cmd.Parameters.Add("@MAG", NpgsqlTypes.NpgsqlDbType.Double);
                                cmd.Parameters.Add("@PLANNER_BUYER", NpgsqlTypes.NpgsqlDbType.Varchar);
                                cmd.Parameters.Add("@RODZAJ", NpgsqlTypes.NpgsqlDbType.Varchar);
                                cmd.Parameters.Add("@CZAS_DOSTAWY", NpgsqlTypes.NpgsqlDbType.Double);
                                cmd.Parameters.Add("@DATA_GWARANCJI", NpgsqlTypes.NpgsqlDbType.Date);
                                cmd.Parameters.Add("@DATA_DOST", NpgsqlTypes.NpgsqlDbType.Date);
                                cmd.Parameters.Add("@PRZYCZYNA", NpgsqlTypes.NpgsqlDbType.Integer);
                                cmd.Parameters.Add("@WLK_DOST", NpgsqlTypes.NpgsqlDbType.Double);
                                cmd.Parameters.Add("@bilans", NpgsqlTypes.NpgsqlDbType.Double);
                                cmd.Parameters.Add("@bil_dost_dzień", NpgsqlTypes.NpgsqlDbType.Double);
                                cmd.Parameters.Add("@DATA_BRAKU", NpgsqlTypes.NpgsqlDbType.Date);
                                cmd.Parameters.Add("@typ_zdarzenia", NpgsqlTypes.NpgsqlDbType.Varchar);
                                cmd.Parameters.Add("@widoczny_od_dnia", NpgsqlTypes.NpgsqlDbType.Timestamp);
                                cmd.Parameters.Add("@sum_dost", NpgsqlTypes.NpgsqlDbType.Double);
                                cmd.Parameters.Add("@sum_potrz", NpgsqlTypes.NpgsqlDbType.Double);
                                cmd.Parameters.Add("@sum_dost_opóźnion", NpgsqlTypes.NpgsqlDbType.Double);
                                cmd.Parameters.Add("@sum_potrz_opóźnion", NpgsqlTypes.NpgsqlDbType.Double);
                                cmd.Parameters.Add("@Status_informacji", NpgsqlTypes.NpgsqlDbType.Varchar);
                                cmd.Parameters.Add("@refr_date", NpgsqlTypes.NpgsqlDbType.Timestamp);
                                cmd.Parameters.Add("@chk", NpgsqlTypes.NpgsqlDbType.Double);
                                cmd.Parameters.Add("@id", NpgsqlTypes.NpgsqlDbType.Uuid);
                                cmd.Prepare();
                                foreach (DataRow row in rw)
                                {
                                    cmd.Parameters[0].Value = row["indeks"];
                                    cmd.Parameters[1].Value = row["OPIS"];
                                    cmd.Parameters[2].Value = row["KOLEKCJA"];
                                    cmd.Parameters[3].Value = row["MAG"];
                                    cmd.Parameters[4].Value = row["PLANNER_BUYER"];
                                    cmd.Parameters[5].Value = row["RODZAJ"];
                                    cmd.Parameters[6].Value = row["CZAS_DOSTAWY"];
                                    cmd.Parameters[7].Value = row["DATA_GWARANCJI"] ?? DBNull.Value;
                                    cmd.Parameters[8].Value = row["DATA_DOST"] ?? DBNull.Value;
                                    cmd.Parameters[9].Value = row["PRZYCZYNA"];
                                    cmd.Parameters[10].Value = row["WLK_DOST"];
                                    cmd.Parameters[11].Value = row["bilans"];
                                    cmd.Parameters[12].Value = row["bil_dost_dzień"];
                                    cmd.Parameters[13].Value = row["DATA_BRAKU"] ?? DBNull.Value;
                                    cmd.Parameters[14].Value = row["typ_zdarzenia"];
                                    cmd.Parameters[15].Value = row["widoczny_od_dnia"] ?? DBNull.Value;
                                    cmd.Parameters[16].Value = row["sum_dost"];
                                    cmd.Parameters[17].Value = row["sum_potrz"];
                                    cmd.Parameters[18].Value = row["sum_dost_opóźnion"];
                                    cmd.Parameters[19].Value = row["sum_potrz_opóźnion"];
                                    cmd.Parameters[20].Value = row["Status_informacji"];
                                    cmd.Parameters[21].Value = row["refr_date"] ?? DBNull.Value;
                                    cmd.Parameters[22].Value = row["chk"];
                                    cmd.Parameters[23].Value = System.Guid.NewGuid();
                                    cmd.ExecuteNonQuery();
                                }
                                rw = null;
                                Log("END INSERT_dta: " + (DateTime.Now - start));
                            }
                        }
                        using (NpgsqlConnection conA = new NpgsqlConnection(npA))
                        {
                            await conA.OpenAsync();
                            using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                                "select cast(count(table_name) as integer) busy " +
                                "from public.datatbles " +
                                "where table_name='demands' and in_progress=true", conA))
                            {
                                int busy_il = 1;
                                while (busy_il > 0)
                                {
                                    busy_il = Convert.ToInt16(cmd.ExecuteScalar());
                                    if (busy_il > 0) { System.Threading.Thread.Sleep(250); }
                                }
                            }
                            conA.Close();
                        }
                        using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                            "UPDATE public.data a " +
                            "SET widoczny_od_dnia=b.ab " +
                            "from " +
                                "(select a.id,min(b.dat_shortage) ab " +
                                "from " +
                                    "(select id,indeks,data_dost,data_braku " +
                                    "from " +
                                        "public.data " +
                                    "where widoczny_od_dnia is null) a," +
                                    "(select part_no,work_day,dat_shortage " +
                                    "from " +
                                        "demands " +
                                        "where dat_shortage is not null) b " +
                                "where b.part_no=a.indeks and  b.work_day between a.data_braku and a.data_dost group by a.id) b " +
                             "where a.id=b.id", conB))
                        {
                            cmd.ExecuteNonQuery();
                        }
                        using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                            "UPDATE public.data b " +
                            "SET status_informacji=a.potw,informacja=a.info " +
                                "from " +
                                "(select a.id,case " +
                                    "when a.typ_zdarzenia in ('Dzisiejsza dostawa','Opóźniona dostawa','Nieaktualne Dostawy') then null " +
                                        "else coalesce(b.rodzaj_potw,'BRAK') end potw," +
                                        "a.status_informacji,b.info " +
                                            "from " +
                                                "public.data a " +
                                                "left join " +
                                                "potw b " +
                                                "on b.indeks=a.indeks and (b.data_dost=a.data_dost or b.rodzaj_potw='NIE ZAMAWIAM') " +
                                             "where " +
                                                "coalesce (a.status_informacji,'N')!=coalesce(case when a.typ_zdarzenia in " +
                                                     "('Dzisiejsza dostawa','Opóźniona dostawa','Nieaktualne Dostawy') then null " +
                                                     "else coalesce(b.rodzaj_potw,'BRAK') end,'N') " +
                                                "or " +
                                                "coalesce(a.informacja,'n')!=coalesce(b.info,'n'))  a  " +
                                            "where b.id=a.id", conB))
                        {
                            cmd.ExecuteNonQuery();
                        }
                        using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                            "UPDATE public.data a " +
                            "SET widoczny_od_dnia=b.refr " +
                            "from (select b.id,b.refr " +
                                "from " +
                                "(select a.id,min(b.dat_shortage) refr " +
                                    "from " +
                                        "(select * from public.data)a," +
                                        "(select part_no,work_day,dat_shortage from demands) b " +
                                    "where b.part_no=a.indeks and b.work_day between a.data_braku and  a.data_dost group by a.id) b," +
                                 "public.data c " +
                                 "where b.id=c.id and b.refr!=c.widoczny_od_dnia) b " +
                             "WHERE a.id=b.id;", conB))
                        {
                            cmd.ExecuteNonQuery();
                        }
                        using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                            "UPDATE public.datatbles " +
                            "SET last_modify=current_timestamp, in_progress=false,updt_errors=false " +
                            "WHERE table_name='data'", conB))
                        {
                            cmd.ExecuteNonQuery();
                        }
                        TR_data.Commit();
                    }
                    Log("END_UPDATE_dta " + (DateTime.Now - start));
                    TASK_dat.Clear();
                    GC.Collect();
                    return 0;
                }
            }
            catch (Exception e)
            {
                using (NpgsqlConnection conA = new NpgsqlConnection(npA))
                {
                    await conA.OpenAsync();
                    using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                        "UPDATE public.datatbles " +
                        "SET in_progress=false,updt_errors=true " +
                        "WHERE table_name='data'", conA))
                    {
                        cmd.ExecuteNonQuery();
                    }
                    conA.Close();
                }
                Log("Błąd modyfikacji tabeli wynikowej dla aplikacji Zakupy:" + e);
                return 1;
            }
        }
        private async Task<int> CUST_ord()
        {
            try
            {
                using (NpgsqlConnection conA = new NpgsqlConnection(npC))
                {
                    await conA.OpenAsync();
                    using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                        "UPDATE public.datatbles " +
                        "SET start_update=current_timestamp, in_progress=true " +
                        "WHERE table_name='cust_ord'", conA))
                    {
                        cmd.ExecuteNonQuery();
                    }
                    conA.Close();
                }


                Log("START cust_ord " + (DateTime.Now - start));
                // Utwórz połączenie z ORACLE
                using (DataTable cust_ord = new DataTable())
                {
                    using (OracleConnection conO = new OracleConnection("Password=pass;User ID = user; Data Source = prod8"))
                    {

                        conO.Open();
                        OracleGlobalization info = conO.GetSessionInfo();
                        info.DateFormat = "YYYY-MM-DD";
                        conO.SetSessionInfo(info);
                        using (OracleCommand mag = new OracleCommand("Select 1 from dual", conO))
                        {
                            // sprawdź połączenie
                            int i = Convert.ToInt32(mag.ExecuteScalar());
                        }
                        //SELECT ifsapp.customer_order_api.Get_Authorize_Code(a.ORDER_NO) KOOR,a.ORDER_NO,a.LINE_NO,a.REL_NO,a.LINE_ITEM_NO,To_Date(c.dat,Decode(InStr(c.dat,'-'),0,'YY/MM/DD','YYYY-MM-DD'))-Delivery_Leadtime Last_Mail_CONF,ifsapp.customer_order_api.Get_Order_Conf(a.ORDER_NO) STATe_conf,a.STATE LINE_STATE,ifsapp.customer_order_api.Get_State(a.ORDER_NO) CUST_ORDER_STATE,ifsapp.customer_order_api.Get_Country_Code(a.ORDER_NO) Country,ifsapp.customer_order_api.Get_Customer_No(a.ORDER_NO) CUST_no,ifsapp.customer_order_address_api.Get_Zip_Code(a.ORDER_NO) ZIP_CODE,ifsapp.customer_order_address_api.Get_Addr_1(a.ORDER_NO)||Decode(Nvl(ifsapp.customer_order_api.Get_Cust_Ref(a.ORDER_NO),''),'','','<<'||ifsapp.customer_order_api.Get_Cust_Ref(a.ORDER_NO)||'>>') ADDR1,Promised_Delivery_Date-Delivery_Leadtime PROM_DATE,To_Char(Promised_Delivery_Date-Delivery_Leadtime,'IYYYIW') PROM_WEEK,LOAD_ID,ifsapp.CUST_ORDER_LOAD_LIST_API.Get_Ship_Date(LOAD_ID) SHIP_DATE,nvl(a.PART_NO,a.CATALOG_NO) PART_NO,nvl(ifsapp.inventory_part_api.Get_Description(CONTRACT,a.PART_NO),a.CATALOG_DESC) Descr,a.CONFIGURATION_ID,a.BUY_QTY_DUE,a.DESIRED_QTY,a.QTY_INVOICED,a.QTY_SHIPPED,a.QTY_ASSIGNED,a.DOP_CONNECTION_DB,nvl(b.dop_id,a.Pre_Accounting_Id) dop_id,ifsapp.dop_head_api.Get_Objstate__(b.dop_id) DOP_STATE,Nvl(ifsapp.dop_order_api.Get_Revised_Due_Date(b.DOP_ID,1),decode(a.DOP_CONNECTION_DB,NULL,a.PLANNED_DUE_DATE)) Data_dop,b.PEGGED_QTY DOP_QTY,Decode(b.QTY_DELIVERED,0,Decode(instr(nvl(ifsapp.dop_head_api.get_C_Trolley_Id(b.dop_id),' '),'-'),0,0,b.PEGGED_QTY),b.QTY_DELIVERED) DOP_MADE,Nvl(b.CREATE_DATE,decode(a.DOP_CONNECTION_DB,NULL,a.DATE_ENTERED)) DATE_ENTERED,owa_opt_lock.checksum(a.OBJVERSION||b.OBJVERSION||nvl(b.dop_id,a.Pre_Accounting_Id)||ifsapp.customer_order_api.Get_Authorize_Code(a.ORDER_NO)||c.dat||ifsapp.customer_order_api.Get_Order_Conf(a.ORDER_NO)||ifsapp.customer_order_api.Get_State(a.ORDER_NO)||ifsapp.customer_order_address_api.Get_Zip_Code(a.ORDER_NO)||ifsapp.customer_order_address_api.Get_Addr_1(a.ORDER_NO)||Decode(Nvl(ifsapp.customer_order_api.Get_Cust_Ref(a.ORDER_NO),''),'','','<<'||ifsapp.customer_order_api.Get_Cust_Ref(a.ORDER_NO)||'>>')||load_id||ifsapp.CUST_ORDER_LOAD_LIST_API.Get_Ship_Date(LOAD_ID)||ifsapp.dop_head_api.Get_Objstate__(b.dop_id)||Decode(b.QTY_DELIVERED,0,Decode(instr(nvl(ifsapp.dop_head_api.get_C_Trolley_Id(b.dop_id),' '),'-'),0,0,b.PEGGED_QTY),b.QTY_DELIVERED)||ifsapp.dop_order_api.Get_Revised_Due_Date(b.DOP_ID,1)) chksum,a.Pre_Accounting_Id custID,null zest,ifsapp.C_Customer_Order_Line_Api.Get_C_Lot0_Flag_Db(a.ORDER_NO,a.LINE_NO,a.REL_NO,a.LINE_ITEM_NO) Seria0,ifsapp.C_Customer_Order_Line_Api.Get_C_Lot0_Date(a.ORDER_NO,a.LINE_NO,a.REL_NO,a.LINE_ITEM_NO) Data0,Upper(a.CUSTOMER_PO_LINE_NO) CUSTOMER_PO_LINE_NO,a.C_DIMENSIONS DIMENSIONS FROM (SELECT a.ORDER_NO||'_'||a.LINE_NO||'_'||a.REL_NO||'_'||a.LINE_ITEM_NO ID,a.* from  ifsapp.customer_order_line a WHERE  a.OBJSTATE NOT IN ('Invoiced','Cancelled','Delivered') ) a left JOIN ifsapp.dop_demand_cust_ord b ON b.ORDER_NO||'_'||b.LINE_NO||'_'||b.REL_NO||'_'||b.LINE_ITEM_NO=a.id left JOIN (SELECT a.ORDER_NO||'_'||a.LINE_NO||'_'||a.REL_NO||'_'||a.LINE_ITEM_NO id,SubStr(Decode(SubStr(a.MESSAGE_TEXT,-1,1),']',a.MESSAGE_TEXT,a.MESSAGE_TEXT||']'),Decode(InStr(a.message_text,'/',-10),0,-11,-9),Decode(InStr(a.message_text,'/',-10),0,10,8)) DAT FROM ifsapp.customer_order_line_hist a,(SELECT Max(HISTORY_NO) hi,a.ORDER_NO,LINE_NO,REL_NO,LINE_ITEM_NO  FROM ifsapp.customer_order_line_hist a,(SELECT order_no FROM ifsapp.customer_order where OBJSTATE NOT IN ('Invoiced','Cancelled','Delivered'))b  WHERE a.order_no=b.order_no AND SubStr(MESSAGE_TEXT,1,3)='Wys'GROUP BY a.ORDER_NO,LINE_NO,REL_NO,LINE_ITEM_NO) b WHERE a.HISTORY_NO=b.HI) c  ON c.id=a.id
                        using (OracleCommand mag = new OracleCommand("" +
                            "SELECT ifsapp.customer_order_api.Get_Authorize_Code(a.ORDER_NO) KOOR,a.ORDER_NO,a.LINE_NO,a.REL_NO,a.LINE_ITEM_NO,a.CUSTOMER_PO_LINE_NO," +
                            "a.C_DIMENSIONS dimmension,To_Date(c.dat,Decode(InStr(c.dat,'-'),0,'YY/MM/DD','YYYY-MM-DD'))-Delivery_Leadtime Last_Mail_CONF," +
                            "ifsapp.customer_order_api.Get_Order_Conf(a.ORDER_NO) STATe_conf,a.STATE LINE_STATE,ifsapp.customer_order_api.Get_State(a.ORDER_NO) CUST_ORDER_STATE," +
                            "ifsapp.customer_order_api.Get_Country_Code(a.ORDER_NO) Country,ifsapp.customer_order_api.Get_Customer_No(a.ORDER_NO) CUST_no," +
                            "ifsapp.customer_order_address_api.Get_Zip_Code(a.ORDER_NO) ZIP_CODE," +
                            "ifsapp.customer_order_address_api.Get_Addr_1(a.ORDER_NO)||Decode(Nvl(ifsapp.customer_order_api.Get_Cust_Ref(a.ORDER_NO),''),'','','<<'||ifsapp.customer_order_api.Get_Cust_Ref(a.ORDER_NO)||'>>') ADDR1," +
                            "Promised_Delivery_Date-Delivery_Leadtime PROM_DATE,To_Char(Promised_Delivery_Date-Delivery_Leadtime,'IYYYIW') PROM_WEEK,LOAD_ID," +
                            "ifsapp.CUST_ORDER_LOAD_LIST_API.Get_Ship_Date(LOAD_ID) SHIP_DATE,nvl(a.PART_NO,a.CATALOG_NO) PART_NO," +
                            "nvl(ifsapp.inventory_part_api.Get_Description(CONTRACT,a.PART_NO),a.CATALOG_DESC) Descr,a.CONFIGURATION_ID,a.BUY_QTY_DUE,a.DESIRED_QTY," +
                            "a.QTY_INVOICED,a.QTY_SHIPPED,a.QTY_ASSIGNED,a.DOP_CONNECTION_DB,nvl(b.dop_id,a.Pre_Accounting_Id) dop_id," +
                            "ifsapp.dop_head_api.Get_Objstate__(b.dop_id) DOP_STATE,Nvl(ifsapp.dop_order_api.Get_Revised_Due_Date(b.DOP_ID,1),decode(a.DOP_CONNECTION_DB,NULL,a.PLANNED_DUE_DATE)) Data_dop," +
                            "b.PEGGED_QTY DOP_QTY," +
                            "Decode(b.QTY_DELIVERED,0,Decode(instr(nvl(ifsapp.dop_head_api.get_C_Trolley_Id(b.dop_id),' '),'-'),0,0," +
                                "Decode(Nvl(LENGTH(TRIM(TRANSLATE(SubStr(ifsapp.dop_head_api.get_C_Trolley_Id(b.dop_id)," +
                                    "instr(ifsapp.dop_head_api.get_C_Trolley_Id(b.dop_id),'-')+1), ' +-.0123456789',' '))),1000),1000,b.PEGGED_QTY,0)),b.QTY_DELIVERED) DOP_MADE," +
                            "Nvl(b.CREATE_DATE,decode(a.DOP_CONNECTION_DB,NULL,a.DATE_ENTERED)) DATE_ENTERED," +
                            "owa_opt_lock.checksum(a.OBJVERSION||b.OBJVERSION||nvl(b.dop_id,a.Pre_Accounting_Id)||ifsapp.customer_order_api.Get_Authorize_Code(a.ORDER_NO)||c.dat||" +
                                "ifsapp.customer_order_api.Get_Order_Conf(a.ORDER_NO)||ifsapp.customer_order_api.Get_State(a.ORDER_NO)||ifsapp.customer_order_address_api.Get_Zip_Code(a.ORDER_NO)||" +
                                "ifsapp.customer_order_address_api.Get_Addr_1(a.ORDER_NO)||Decode(Nvl(ifsapp.customer_order_api.Get_Cust_Ref(a.ORDER_NO),''),'','','<<'||" +
                                "ifsapp.customer_order_api.Get_Cust_Ref(a.ORDER_NO)||'>>')||load_id||ifsapp.CUST_ORDER_LOAD_LIST_API.Get_Ship_Date(LOAD_ID)||ifsapp.dop_head_api.Get_Objstate__(b.dop_id)||" +
                                "Decode(b.QTY_DELIVERED,0,Decode(instr(nvl(ifsapp.dop_head_api.get_C_Trolley_Id(b.dop_id),' '),'-'),0,0,b.PEGGED_QTY),b.QTY_DELIVERED)||" +
                                "ifsapp.dop_order_api.Get_Revised_Due_Date(b.DOP_ID,1)) chksum," +
                             "a.Pre_Accounting_Id custID,null zest,ifsapp.C_Customer_Order_Line_Api.Get_C_Lot0_Flag_Db(a.ORDER_NO,a.LINE_NO,a.REL_NO,a.LINE_ITEM_NO) Seria0," +
                             "ifsapp.C_Customer_Order_Line_Api.Get_C_Lot0_Date(a.ORDER_NO,a.LINE_NO,a.REL_NO,a.LINE_ITEM_NO) Data0 " +
                             "FROM " +
                                "(SELECT a.ORDER_NO||'_'||a.LINE_NO||'_'||a.REL_NO||'_'||a.LINE_ITEM_NO ID,a.* " +
                                "from  " +
                                    "ifsapp.customer_order_line a " +
                                "WHERE  a.OBJSTATE NOT IN ('Invoiced','Cancelled','Delivered') ) a " +
                                "left JOIN " +
                                "ifsapp.dop_demand_cust_ord b " +
                                "ON b.ORDER_NO||'_'||b.LINE_NO||'_'||b.REL_NO||'_'||b.LINE_ITEM_NO=a.id " +
                                "left JOIN " +
                                "(SELECT a.ORDER_NO||'_'||a.LINE_NO||'_'||a.REL_NO||'_'||a.LINE_ITEM_NO id," +
                                "SubStr(Decode(SubStr(a.MESSAGE_TEXT,-1,1),']',a.MESSAGE_TEXT,a.MESSAGE_TEXT||']'),Decode(InStr(a.message_text,'/',-10,2),0,-11,-9)," +
                                        "Decode(InStr(a.message_text,'/',-10,2),0,10,8)) DAT " +
                                "FROM " +
                                    "ifsapp.customer_order_line_hist a," +
                                    "(SELECT Max(HISTORY_NO) hi,a.ORDER_NO,LINE_NO,REL_NO,LINE_ITEM_NO  " +
                                    "FROM " +
                                        "ifsapp.customer_order_line_hist a," +
                                        "(SELECT order_no FROM ifsapp.customer_order where OBJSTATE NOT IN ('Invoiced','Cancelled','Delivered'))b  " +
                                    "WHERE a.order_no=b.order_no AND SubStr(MESSAGE_TEXT,1,3)='Wys'" +
                                    "GROUP BY a.ORDER_NO,LINE_NO,REL_NO,LINE_ITEM_NO) b " +
                                    "WHERE a.HISTORY_NO=b.HI) c  " +
                                 "ON c.id=a.id", conO))
                        {
                            using (OracleDataReader re = mag.ExecuteReader())
                            {
                                re.FetchSize = mag.RowSize * 500;
                                Log("Cust_ord start_load FetchSize:" + mag.FetchSize + " Started:" + (DateTime.Now - start));
                                cust_ord.Load(re);
                                cust_ord.DefaultView.Sort = "custid ASC";
                                Log("Cust_ord end_retieve " + (DateTime.Now - start));
                            }
                        }
                    }
                    using (DataTable P_cust_ord = new DataTable())
                    {
                        using (NpgsqlConnection conA = new NpgsqlConnection(npA))
                        {
                            await conA.OpenAsync();
                            using (NpgsqlCommand pot = new NpgsqlCommand("Select * from cust_ord order by custid ASC", conA))
                            {
                                using (NpgsqlDataReader po = pot.ExecuteReader())
                                {
                                    using (DataTable sch = po.GetSchemaTable())
                                    {
                                        foreach (DataRow a in sch.Rows)
                                        {
                                            P_cust_ord.Columns.Add(a["ColumnName"].ToString(), System.Type.GetType(a["DataType"].ToString()));
                                            P_cust_ord.Columns[a["ColumnName"].ToString()].AllowDBNull = true;
                                        }
                                    }
                                    P_cust_ord.Load(po);
                                    P_cust_ord.Columns.Remove("objversion");
                                }
                            }
                            conA.Close();
                        }
                        using (DataTable TMP_Pcustord = P_cust_ord.Clone())
                        {
                            TMP_Pcustord.Columns.Remove("ID");
                            TMP_Pcustord.Columns.Add("SRC_ID", System.Type.GetType("System.Guid"));
                            TMP_Pcustord.Columns.Add("Czynnosc");
                            TMP_Pcustord.Columns.Add("STATUS");
                            Log("REaDY UPDATE cust_ord " + (DateTime.Now - start));
                            int cust_count = 0;
                            int max_cust_ord = P_cust_ord.Rows.Count;
                            foreach (DataRowView rek in cust_ord.DefaultView)
                            {
                                if (max_cust_ord > cust_count)
                                {
                                    while (Convert.ToInt32(rek["custid"]) > (int)P_cust_ord.DefaultView[cust_count].Row["custid"])
                                    {
                                        DataRow rw = TMP_Pcustord.NewRow();
                                        rw["Src_id"] = P_cust_ord.DefaultView[cust_count].Row["ID"];
                                        rw["Czynnosc"] = "DEL_ID_DEM";
                                        rw["Status"] = "Zapl.";
                                        TMP_Pcustord.Rows.Add(rw);
                                        cust_count++;
                                        if (max_cust_ord <= cust_count) { break; }
                                    }
                                }
                                if (max_cust_ord > cust_count)
                                {
                                    if (Convert.ToInt32(rek["custid"]) == (int)P_cust_ord.DefaultView[cust_count].Row["custid"])
                                    {
                                        bool updt = false;
                                        bool dta0 = false;
                                        bool dta1 = false;
                                        if (rek["data0"].ToString() == "") { dta0 = true; }
                                        if (P_cust_ord.DefaultView[cust_count].Row["data0"].ToString() == "") { dta1 = true; }
                                        if (Convert.ToInt32(rek["chksum"]) != (int)P_cust_ord.DefaultView[cust_count].Row["chksum"] | Convert.ToBoolean(rek["seria0"]) != (bool)P_cust_ord.DefaultView[cust_count].Row["seria0"])
                                        {
                                            updt = true;
                                        }
                                        else if (dta0 != dta1)
                                        {
                                            if (P_cust_ord.DefaultView[cust_count].Row["data0"] != rek["data0"])
                                            {
                                                updt = true;
                                            }
                                        }
                                        else if (dta0 == false && dta1 == false)
                                        {
                                            if (Convert.ToDateTime(P_cust_ord.DefaultView[cust_count].Row["data0"]).Date != Convert.ToDateTime(rek["data0"]).Date)
                                            {
                                                updt = true;
                                            }
                                        }
                                        if (!updt)
                                        {
                                            if (!updt && P_cust_ord.DefaultView[cust_count].Row["state_conf"].ToString() != rek["state_conf"].ToString())
                                            {
                                                updt = true;
                                            }
                                            if (!updt && P_cust_ord.DefaultView[cust_count].Row["last_mail_conf"].ToString() != rek["last_mail_conf"].ToString())
                                            {
                                                updt = true;
                                            }
                                        }
                                        if (!updt)
                                        {
                                            if (!updt && P_cust_ord.DefaultView[cust_count].Row["customer_po_line_no"].ToString() != rek["customer_po_line_no"].ToString())
                                            {
                                                updt = true;
                                            }
                                        }
                                        if (!updt)
                                        {
                                            if (P_cust_ord.DefaultView[cust_count].Row["dimmension"] == DBNull.Value)
                                            {
                                                if (rek["dimmension"] != DBNull.Value)
                                                {
                                                    updt = true;
                                                }
                                            }
                                            else
                                            {
                                                if (!updt && Convert.ToDouble(P_cust_ord.DefaultView[cust_count].Row["dimmension"]) != Convert.ToDouble(rek["dimmension"]))
                                                {
                                                    updt = true;
                                                }
                                            }
                                        }
                                        if (!updt)
                                        {
                                            if (!updt && P_cust_ord.DefaultView[cust_count].Row["line_state"].ToString() != rek["line_state"].ToString())
                                            {
                                                updt = true;
                                            }
                                            if (!updt && P_cust_ord.DefaultView[cust_count].Row["cust_order_state"].ToString() != rek["cust_order_state"].ToString())
                                            {
                                                updt = true;
                                            }
                                        }
                                        if (!updt)
                                        {
                                            if (!updt && Convert.ToInt32(P_cust_ord.DefaultView[cust_count].Row["prom_week"]) != Convert.ToInt32(rek["prom_week"]))
                                            {
                                                updt = true;
                                            }
                                            if (!updt && P_cust_ord.DefaultView[cust_count].Row["addr1"].ToString() != rek["addr1"].ToString())
                                            {
                                                updt = true;
                                            }
                                        }
                                        if (!updt)
                                        {
                                            if (!updt && P_cust_ord.DefaultView[cust_count].Row["load_id"].ToString() != rek["load_id"].ToString())
                                            {
                                                updt = true;
                                            }
                                            if (!updt && Convert.ToDateTime(P_cust_ord.DefaultView[cust_count].Row["prom_date"]).Date != Convert.ToDateTime(rek["prom_date"]).Date)
                                            {
                                                updt = true;
                                            }
                                        }
                                        if (!updt)
                                        {
                                            if (!updt && P_cust_ord.DefaultView[cust_count].Row["last_mail_conf"].ToString() == "")
                                            {
                                                if (rek["last_mail_conf"].ToString() != "")
                                                {
                                                    updt = true;
                                                }
                                            }
                                            else
                                            {
                                                if (rek["last_mail_conf"].ToString() == "")
                                                {
                                                    updt = true;
                                                }
                                                else
                                                {
                                                    if (Convert.ToDateTime(P_cust_ord.DefaultView[cust_count].Row["last_mail_conf"]) != Convert.ToDateTime(rek["last_mail_conf"]).Date)
                                                    {
                                                        updt = true;
                                                    }
                                                }
                                            }
                                        }
                                        if (!updt)
                                        {
                                            if (!updt && P_cust_ord.DefaultView[cust_count].Row["ship_date"].ToString() == "" || P_cust_ord.DefaultView[cust_count].Row["ship_date"] == DBNull.Value)
                                            {
                                                if (rek["ship_date"].ToString() != "")
                                                {
                                                    updt = true;
                                                }
                                            }
                                            else
                                            {
                                                if (rek["ship_date"].ToString() == "" || rek["ship_date"] == DBNull.Value)
                                                {
                                                    updt = true;
                                                }
                                                else
                                                {
                                                    if (Convert.ToDateTime(P_cust_ord.DefaultView[cust_count].Row["ship_date"]).Date != Convert.ToDateTime(rek["ship_date"]).Date)
                                                    {
                                                        updt = true;
                                                    }
                                                }
                                            }
                                        }
                                        if (!updt)
                                        {
                                            if (!updt && Convert.ToInt32(P_cust_ord.DefaultView[cust_count].Row["dop_id"]) != Convert.ToInt32(rek["dop_id"]))
                                            {
                                                updt = true;
                                            }
                                            if (!updt && P_cust_ord.DefaultView[cust_count].Row["dop_state"].ToString() != rek["dop_state"].ToString())
                                            {
                                                updt = true;
                                            }
                                            if (!updt && P_cust_ord.DefaultView[cust_count].Row["data_dop"].ToString() == "" || P_cust_ord.DefaultView[cust_count].Row["data_dop"] == DBNull.Value)
                                            {
                                                if (rek["data_dop"].ToString() != "")
                                                {
                                                    updt = true;
                                                }
                                            }
                                            else
                                            {
                                                if (rek["data_dop"].ToString() == "" || rek["data_dop"] == DBNull.Value)
                                                {
                                                    updt = true;
                                                }
                                                else
                                                {
                                                    if (Convert.ToDateTime(P_cust_ord.DefaultView[cust_count].Row["data_dop"]).Date != Convert.ToDateTime(rek["data_dop"]).Date)
                                                    {
                                                        updt = true;
                                                    }
                                                }
                                            }
                                        }

                                        if (updt)
                                        {
                                            DataRow rw = TMP_Pcustord.NewRow();
                                            for (int i = 0; i < P_cust_ord.Columns.Count - 1; i++)
                                            {
                                                rw[i] = rek[i];
                                            }
                                            rw["src_id"] = P_cust_ord.DefaultView[cust_count].Row["id"];
                                            rw["Czynnosc"] = "MoD_DEM";
                                            rw["Status"] = "Zapl.";
                                            TMP_Pcustord.Rows.Add(rw);
                                        }
                                        cust_count++;
                                    }
                                    else
                                    {
                                        DataRow rw = TMP_Pcustord.NewRow();
                                        for (int i = 0; i < P_cust_ord.Columns.Count - 1; i++)
                                        {
                                            rw[i] = rek[i];
                                        }
                                        rw["Src_id"] = System.Guid.NewGuid();
                                        rw["Czynnosc"] = "ADD_NEW_DEM";
                                        rw["Status"] = "Zapl.";
                                        TMP_Pcustord.Rows.Add(rw);
                                    }
                                }
                                else
                                {
                                    DataRow rw = TMP_Pcustord.NewRow();
                                    for (int i = 0; i < P_cust_ord.Columns.Count - 1; i++)
                                    {
                                        rw[i] = rek[i];
                                    }
                                    rw["Src_id"] = System.Guid.NewGuid();
                                    rw["Czynnosc"] = "ADD_NEW_DEM";
                                    rw["Status"] = "Zapl.";
                                    TMP_Pcustord.Rows.Add(rw);
                                }
                            }
                            using (NpgsqlConnection conB = new NpgsqlConnection(npA))
                            {
                                Log("START UPDATE_cust: " + (DateTime.Now - start));
                                await conB.OpenAsync();
                                using (NpgsqlTransaction TR_CUSTORD = conB.BeginTransaction())
                                {
                                    Log("START DELETE_GIUD_cust: " + (DateTime.Now - start));
                                    DataRow[] rw = TMP_Pcustord.Select("Czynnosc= 'DEL_ID_DEM' and Status='Zapl.'");
                                    Log("RECORDS DELETE_cust: " + rw.Length);
                                    if (rw.Length > 0)
                                    {
                                        using (NpgsqlCommand cmd = new NpgsqlCommand("DELETE from cust_ord where \"id\"=@ID", conB))
                                        {
                                            cmd.Parameters.Add("@ID", NpgsqlTypes.NpgsqlDbType.Uuid);
                                            cmd.Prepare();
                                            foreach (DataRow row in rw)
                                            {
                                                cmd.Parameters[0].Value = row["Src_ID"];
                                                cmd.ExecuteNonQuery();
                                            }
                                            rw = null;
                                            Log("END DELETE_GIUD_cust: " + (DateTime.Now - start));
                                        }
                                    }
                                    Log("Start Modify_cust: " + (DateTime.Now - start));
                                    rw = TMP_Pcustord.Select("Czynnosc= 'MoD_DEM' and Status='Zapl.'");
                                    Log("RECORDS Modify cust: " + rw.Length);
                                    if (rw.Length > 0)
                                    {
                                        using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                                            "UPDATE public.cust_ord " +
                                            "SET koor=@koor, order_no=@order_no, line_no=@line_no, rel_no=@rel_no, line_item_no=@line_item_no, " +
                                                "customer_po_line_no=@customer_po_line_no, dimmension=@dimmension , last_mail_conf=@last_mail_conf, state_conf=@state_conf," +
                                                " line_state=@line_state, cust_order_state=@cust_order_state, country=@country, cust_no=@cust_no, zip_code=@zip_code, addr1=@addr1," +
                                                " prom_date=@prom_date, prom_week=@prom_week, load_id=@load_id, ship_date=@ship_date, part_no=@part_no, descr=@descr," +
                                                " configuration=@configuration, buy_qty_due=@buy_qty_due, desired_qty=@desired_qty, qty_invoiced=@qty_invoiced," +
                                                " qty_shipped=@qty_shipped, qty_assigned=@qty_assigned, dop_connection_db=@dop_connection_db, dop_id=@dop_id," +
                                                " dop_state=@dop_state, data_dop=@data_dop, dop_qty=@dop_qty, dop_made=@dop_made, date_entered=@date_entered, chksum=@chksum," +
                                                " custid=@custid,zest=@zest,seria0=@seria0,data0=@data0,objversion=current_timestamp " +
                                             "where \"id\"=@id;", conB))
                                        {
                                            cmd.Parameters.Add("@koor", NpgsqlTypes.NpgsqlDbType.Varchar);
                                            cmd.Parameters.Add("@order_no", NpgsqlTypes.NpgsqlDbType.Varchar);
                                            cmd.Parameters.Add("@line_no", NpgsqlTypes.NpgsqlDbType.Varchar);
                                            cmd.Parameters.Add("@rel_no", NpgsqlTypes.NpgsqlDbType.Varchar);
                                            cmd.Parameters.Add("@line_item_no", NpgsqlTypes.NpgsqlDbType.Integer);
                                            cmd.Parameters.Add("@customer_po_line_no", NpgsqlTypes.NpgsqlDbType.Varchar);
                                            cmd.Parameters.Add("@dimmension", NpgsqlTypes.NpgsqlDbType.Double);
                                            cmd.Parameters.Add("@last_mail_conf", NpgsqlTypes.NpgsqlDbType.Date);
                                            cmd.Parameters.Add("@state_conf", NpgsqlTypes.NpgsqlDbType.Varchar);
                                            cmd.Parameters.Add("@line_state", NpgsqlTypes.NpgsqlDbType.Varchar);
                                            cmd.Parameters.Add("@cust_order_state", NpgsqlTypes.NpgsqlDbType.Varchar);
                                            cmd.Parameters.Add("@country", NpgsqlTypes.NpgsqlDbType.Varchar);
                                            cmd.Parameters.Add("@cust_no", NpgsqlTypes.NpgsqlDbType.Varchar);
                                            cmd.Parameters.Add("@zip_code", NpgsqlTypes.NpgsqlDbType.Varchar);
                                            cmd.Parameters.Add("@addr1", NpgsqlTypes.NpgsqlDbType.Varchar);
                                            cmd.Parameters.Add("@prom_date", NpgsqlTypes.NpgsqlDbType.Date);
                                            cmd.Parameters.Add("@prom_week", NpgsqlTypes.NpgsqlDbType.Varchar);
                                            cmd.Parameters.Add("@load_id", NpgsqlTypes.NpgsqlDbType.Integer);
                                            cmd.Parameters.Add("@ship_date", NpgsqlTypes.NpgsqlDbType.Date);
                                            cmd.Parameters.Add("@part_no", NpgsqlTypes.NpgsqlDbType.Varchar);
                                            cmd.Parameters.Add("@descr", NpgsqlTypes.NpgsqlDbType.Varchar);
                                            cmd.Parameters.Add("@configuration", NpgsqlTypes.NpgsqlDbType.Varchar);
                                            cmd.Parameters.Add("@buy_qty_due", NpgsqlTypes.NpgsqlDbType.Double);
                                            cmd.Parameters.Add("@desired_qty", NpgsqlTypes.NpgsqlDbType.Double);
                                            cmd.Parameters.Add("@qty_invoiced", NpgsqlTypes.NpgsqlDbType.Double);
                                            cmd.Parameters.Add("@qty_shipped", NpgsqlTypes.NpgsqlDbType.Double);
                                            cmd.Parameters.Add("@qty_assigned", NpgsqlTypes.NpgsqlDbType.Double);
                                            cmd.Parameters.Add("@dop_connection_db", NpgsqlTypes.NpgsqlDbType.Varchar);
                                            cmd.Parameters.Add("@dop_id", NpgsqlTypes.NpgsqlDbType.Integer);
                                            cmd.Parameters.Add("@dop_state", NpgsqlTypes.NpgsqlDbType.Varchar);
                                            cmd.Parameters.Add("@data_dop", NpgsqlTypes.NpgsqlDbType.Date);
                                            cmd.Parameters.Add("@dop_qty", NpgsqlTypes.NpgsqlDbType.Double);
                                            cmd.Parameters.Add("@dop_made", NpgsqlTypes.NpgsqlDbType.Double);
                                            cmd.Parameters.Add("@date_entered", NpgsqlTypes.NpgsqlDbType.Timestamp);
                                            cmd.Parameters.Add("@chksum", NpgsqlTypes.NpgsqlDbType.Integer);
                                            cmd.Parameters.Add("@custid", NpgsqlTypes.NpgsqlDbType.Integer);
                                            cmd.Parameters.Add("@zest", NpgsqlTypes.NpgsqlDbType.Varchar);
                                            cmd.Parameters.Add("@seria0", NpgsqlTypes.NpgsqlDbType.Boolean);
                                            cmd.Parameters.Add("@data0", NpgsqlTypes.NpgsqlDbType.Date);
                                            cmd.Parameters.Add("@id", NpgsqlTypes.NpgsqlDbType.Uuid);
                                            cmd.Prepare();
                                            foreach (DataRow row in rw)
                                            {
                                                for (int i = 0; i < P_cust_ord.Columns.Count; i++)
                                                {
                                                    cmd.Parameters[i].Value = row[i] ?? DBNull.Value;
                                                }
                                                cmd.ExecuteNonQuery();
                                            }
                                            rw = null;
                                            Log("END Modify_cust: " + (DateTime.Now - start));
                                        }
                                    }
                                    Log("START INSERT_cust: " + (DateTime.Now - start));
                                    rw = TMP_Pcustord.Select("Czynnosc= 'ADD_NEW_DEM' and Status='Zapl.'");
                                    Log("RECORDS INSERT cust: " + rw.Length);
                                    if (rw.Length > 0)
                                    {
                                        using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                                            "INSERT INTO public.cust_ord" +
                                            "(koor, order_no, line_no, rel_no, line_item_no,customer_po_line_no,dimmension, last_mail_conf, state_conf, line_state," +
                                                " cust_order_state, country, cust_no, zip_code, addr1, prom_date, prom_week, load_id, ship_date, part_no, descr," +
                                                " configuration, buy_qty_due, desired_qty, qty_invoiced, qty_shipped, qty_assigned, dop_connection_db, dop_id, dop_state," +
                                                " data_dop, dop_qty, dop_made, date_entered, chksum, custid, id,zest,seria0,data0,objversion) " +
                                            "VALUES " +
                                            "(@koor, @order_no, @line_no, @rel_no, @line_item_no,@customer_po_line_no,@dimmension, @last_mail_conf, @state_conf, @line_state," +
                                            " @cust_order_state, @country, @cust_no, @zip_code, @addr1, @prom_date, @prom_week, @load_id, @ship_date, @part_no, @descr," +
                                            " @configuration, @buy_qty_due, @desired_qty, @qty_invoiced, @qty_shipped, @qty_assigned, @dop_connection_db, @dop_id, @dop_state," +
                                            " @data_dop, @dop_qty, @dop_made, @date_entered, @chksum, @custid, @id,@zest,@seria0,@data0,current_timestamp);", conB))
                                        {
                                            cmd.Parameters.Add("@koor", NpgsqlTypes.NpgsqlDbType.Varchar);
                                            cmd.Parameters.Add("@order_no", NpgsqlTypes.NpgsqlDbType.Varchar);
                                            cmd.Parameters.Add("@line_no", NpgsqlTypes.NpgsqlDbType.Varchar);
                                            cmd.Parameters.Add("@rel_no", NpgsqlTypes.NpgsqlDbType.Varchar);
                                            cmd.Parameters.Add("@line_item_no", NpgsqlTypes.NpgsqlDbType.Integer);
                                            cmd.Parameters.Add("@customer_po_line_no", NpgsqlTypes.NpgsqlDbType.Varchar);
                                            cmd.Parameters.Add("@dimmension", NpgsqlTypes.NpgsqlDbType.Double);
                                            cmd.Parameters.Add("@last_mail_conf", NpgsqlTypes.NpgsqlDbType.Date);
                                            cmd.Parameters.Add("@state_conf", NpgsqlTypes.NpgsqlDbType.Varchar);
                                            cmd.Parameters.Add("@line_state", NpgsqlTypes.NpgsqlDbType.Varchar);
                                            cmd.Parameters.Add("@cust_order_state", NpgsqlTypes.NpgsqlDbType.Varchar);
                                            cmd.Parameters.Add("@country", NpgsqlTypes.NpgsqlDbType.Varchar);
                                            cmd.Parameters.Add("@cust_no", NpgsqlTypes.NpgsqlDbType.Varchar);
                                            cmd.Parameters.Add("@zip_code", NpgsqlTypes.NpgsqlDbType.Varchar);
                                            cmd.Parameters.Add("@addr1", NpgsqlTypes.NpgsqlDbType.Varchar);
                                            cmd.Parameters.Add("@prom_date", NpgsqlTypes.NpgsqlDbType.Date);
                                            cmd.Parameters.Add("@prom_week", NpgsqlTypes.NpgsqlDbType.Varchar);
                                            cmd.Parameters.Add("@load_id", NpgsqlTypes.NpgsqlDbType.Integer);
                                            cmd.Parameters.Add("@ship_date", NpgsqlTypes.NpgsqlDbType.Date);
                                            cmd.Parameters.Add("@part_no", NpgsqlTypes.NpgsqlDbType.Varchar);
                                            cmd.Parameters.Add("@descr", NpgsqlTypes.NpgsqlDbType.Varchar);
                                            cmd.Parameters.Add("@configuration", NpgsqlTypes.NpgsqlDbType.Varchar);
                                            cmd.Parameters.Add("@buy_qty_due", NpgsqlTypes.NpgsqlDbType.Double);
                                            cmd.Parameters.Add("@desired_qty", NpgsqlTypes.NpgsqlDbType.Double);
                                            cmd.Parameters.Add("@qty_invoiced", NpgsqlTypes.NpgsqlDbType.Double);
                                            cmd.Parameters.Add("@qty_shipped", NpgsqlTypes.NpgsqlDbType.Double);
                                            cmd.Parameters.Add("@qty_assigned", NpgsqlTypes.NpgsqlDbType.Double);
                                            cmd.Parameters.Add("@dop_connection_db", NpgsqlTypes.NpgsqlDbType.Varchar);
                                            cmd.Parameters.Add("@dop_id", NpgsqlTypes.NpgsqlDbType.Integer);
                                            cmd.Parameters.Add("@dop_state", NpgsqlTypes.NpgsqlDbType.Varchar);
                                            cmd.Parameters.Add("@data_dop", NpgsqlTypes.NpgsqlDbType.Date);
                                            cmd.Parameters.Add("@dop_qty", NpgsqlTypes.NpgsqlDbType.Double);
                                            cmd.Parameters.Add("@dop_made", NpgsqlTypes.NpgsqlDbType.Double);
                                            cmd.Parameters.Add("@date_entered", NpgsqlTypes.NpgsqlDbType.Timestamp);
                                            cmd.Parameters.Add("@chksum", NpgsqlTypes.NpgsqlDbType.Integer);
                                            cmd.Parameters.Add("@custid", NpgsqlTypes.NpgsqlDbType.Integer);
                                            cmd.Parameters.Add("@zest", NpgsqlTypes.NpgsqlDbType.Varchar);
                                            cmd.Parameters.Add("@seria0", NpgsqlTypes.NpgsqlDbType.Boolean);
                                            cmd.Parameters.Add("@data0", NpgsqlTypes.NpgsqlDbType.Date);
                                            cmd.Parameters.Add("@id", NpgsqlTypes.NpgsqlDbType.Uuid);
                                            cmd.Prepare();
                                            foreach (DataRow row in rw)
                                            {
                                                for (int i = 0; i < P_cust_ord.Columns.Count; i++)
                                                {
                                                    cmd.Parameters[i].Value = row[i] ?? DBNull.Value;
                                                }
                                                cmd.ExecuteNonQuery();
                                            }
                                            rw = null;
                                            Log("END INSERT cust : " + (DateTime.Now - start));
                                        }
                                    }
                                    using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                                        "update public.cust_ord a " +
                                        "SET zest=case when a.dop_connection_db = 'AUT' then " +
                                            "case when a.line_state='Aktywowana' then " +
                                                "case when dop_made=0 then " +
                                                    "case when substring(a.part_no,1,1) not in ('5','6','2') then b.zs " +
                                                    "else null	end " +
                                                 "else null end " +
                                             "else null end else null end " +
                                             "from " +
                                                "(select ZEST_ID,CASE WHEN zest>1 THEN zest_id ELSE null END as zs " +
                                                    "from " +
                                                    "(select a.order_no,a.line_no,b.zest,a.order_no||'_'||coalesce(a.customer_po_line_no,a.line_no)||'_'||a.prom_week ZEST_ID " +
                                                        "from " +
                                                        "cust_ord a " +
                                                        "left join " +
                                                        "(select id,count(zest) zest " +
                                                            "from " +
                                                            "(select order_no||'_'||coalesce(customer_po_line_no,line_no)||'_'||prom_week id,part_no zest " +
                                                                "from cust_ord " +
                                                                "where line_state!='Zarezerwowana' and dop_connection_db='AUT' and seria0=false " +
                                                                    "and data0 is null group by order_no||'_'||coalesce(customer_po_line_no,line_no)||'_'||prom_week,part_no ) a " +
                                                           "group by id) b " +
                                                         "on b.id=a.order_no||'_'||coalesce(a.customer_po_line_no,a.line_no)||'_'||a.prom_week " +
                                                     "where substring(part_no,1,1) not in ('5','6','2') ) a) b " +
                                                 "where a.order_no||'_'||coalesce(a.customer_po_line_no,a.line_no)||'_'||a.prom_week=b.ZEST_ID", conB))
                                    {
                                        Log("Cust_ord update zest" + (DateTime.Now - start));
                                        cmd.ExecuteNonQuery();
                                    }
                                    using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                                        "UPDATE public.datatbles " +
                                        "SET last_modify=current_timestamp, in_progress=false,updt_errors=false " +
                                        "WHERE table_name='cust_ord'", conB))
                                    {
                                        cmd.ExecuteNonQuery();
                                    }
                                    TR_CUSTORD.Commit();
                                }
                                conB.Close();
                            }
                            Log("REaDY cust_ord " + (DateTime.Now - start));
                        }
                    }
                }
                GC.Collect();
                return 0;
            }
            catch (Exception e)
            {
                using (NpgsqlConnection conA = new NpgsqlConnection(npC))
                {
                    await conA.OpenAsync();
                    using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                        "UPDATE public.datatbles " +
                        "SET in_progress=false,updt_errors=true " +
                        "WHERE table_name='cust_ord'", conA))
                    {
                        cmd.ExecuteNonQuery();
                    }
                    conA.Close();
                }
                Log("Błąd modyfikacji tabeli Cust_ord:" + e);
                return 1;
            }
        }
        private async Task<int> Get_ord_dem()
        {
            try
            {
                using (OracleConnection conO = new OracleConnection("Password = pass;User ID = user; Data Source = prod8"))
                {
                    await conO.OpenAsync();
                    using (NpgsqlConnection conB = new NpgsqlConnection(npA))
                    {
                        conB.Open();
                        using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                            "UPDATE public.datatbles " +
                            "SET start_update=current_timestamp, in_progress=true " +
                            "WHERE table_name='worker1'", conB))
                        {
                            cmd.ExecuteNonQuery();
                        }
                        Log("start get_ord" + (DateTime.Now - start));
                        using (NpgsqlTransaction TR_updt = conB.BeginTransaction())
                        {
                            using (OracleCommand cmd = new OracleCommand("" +
                                "SELECT  a.DOP,Nvl(a.LINE_ITEM_NO,a.DOP_LIN) DOP_LIN,Nvl(ifsapp.dop_order_api.Get_Revised_Due_Date(a.DOP,1),a.DATE_REQUIRED) Data_dop," +
                                "Nvl(ifsapp.work_time_calendar_api.Get_Work_Days_Between(ifsapp.site_api.Get_Manuf_Calendar_Id(a.CONTRACT)," +
                                    " ifsapp.dop_order_api.Get_Revised_Start_Date(a.DOP, a.DOP_LIN), " +
                                    "ifsapp.dop_order_api.Get_Revised_Due_Date(a.DOP, 1)) + ifsapp.dop_order_api.Dop_Order_Slack(a.DOP, a.DOP_LIN) + 1, 0) DAy_shift," +
                                "a.ORDER_NO, a.LINE_NO, a.REL_NO," +
                                "cast(Decode(LENGTH(TRIM(TRANSLATE(a.ORDER_NO, ' +-.0123456789',' '))),NULL,a.ORDER_NO,owa_opt_lock.checksum(a.ORDER_NO)*-1) as INT) int_ord," +
                                " a.CONTRACT, a.ORDER_SUPPLY_DEMAND_TYPE, a.WRKC, a.NEXT_WRKC, a.PART_NO, ifsapp.inventory_part_api.Get_Description(CONTRACT, a.PART_NO) Descr," +
                                " a.PART_CODE, a.DATE_REQUIRED,a.ORD_STATE, a.ORD_DATE,cast(a.PROD_QTY as float) PROD_QTY,cast(a.QTY_SUPPLY as float) QTY_SUPPLY," +
                                "cast(a.QTY_DEMAND as float) QTY_DEMAND, To_Date(creat_date) creat_date,chksum " +
                                    "from " +
                                    "(SELECT Nvl(ifsapp.dop_supply_shop_ord_api.Get_C_Dop_Id(order_no, line_no, rel_no), 0) DOP," +
                                    "Nvl(REPLACE(SubStr(ifsapp.shop_ord_api.Get_Source(order_no, line_no, rel_no), " +
                                        "InStr(ifsapp.shop_ord_api.Get_Source(order_no, line_no, rel_no), '^', 10)), '^', ''), 0) DOP_LIN," +
                                    "order_no, line_no, rel_no, LINE_ITEM_NO, ifsapp.shop_ord_api.Get_Part_No(order_no, line_no, rel_no) Cust_part_no," +
                                    " CONTRACT, ORDER_SUPPLY_DEMAND_TYPE,ifsapp.shop_order_operation_list_api.Get_Next_Op_Work_Center(order_no, line_no, rel_no," +
                                        " ifsapp.shop_order_operation_list_api.Get_Prev_Non_Parallel_Op(order_no, line_no, rel_no, 1, 0)) WRKC," +
                                    "ifsapp.shop_order_operation_list_api.Get_Next_Op_Work_Center(order_no, line_no, rel_no, " +
                                        "ifsapp.shop_order_operation_list_api.Get_Prev_Non_Parallel_Op(order_no, line_no, rel_no, 2, 0)) NEXT_WRKC," +
                                        "PART_NO, STATUS_CODE PART_CODE, DATE_REQUIRED, ifsapp.shop_ord_api.Get_State(order_no, line_no, rel_no) ORD_STATE," +
                                    " ifsapp.shop_ord_api.Get_Revised_Due_Date(order_no, line_no, rel_no) ORD_date," +
                                    "Nvl(ifsapp.shop_ord_api.Get_Revised_Qty_Due(order_no, line_no, rel_no)-ifsapp.shop_ord_api.Get_Qty_Complete(order_no, line_no, rel_no), 0) Prod_QTY," +
                                    "0 QTY_SUPPLY, QTY_DEMAND, ifsapp.shop_ord_api.Get_Date_Entered(order_no, line_no, rel_no) creat_date," +
                                    " owa_opt_lock.checksum(ROWID||QTY_DEMAND||QTY_PEGGED||QTY_RESERVED||" +
                                        "To_Char(ifsapp.shop_order_operation_api.Get_Op_Start_Date(order_no,line_no,rel_no," +
                                            "ifsapp.shop_order_operation_list_api.Get_Prev_Non_Parallel_Op (order_no,line_no,rel_no,2,0)),'YYYYMMDDHH24miss')) chksum  " +
                                     "FROM " +
                                     "ifsapp.shop_material_alloc_demand " +
                                        "WHERE part_no = :part_no AND DATE_REQUIRED between :Dat and :Dat1 " +
                                     "UNION ALL " +
                                     "sELECT To_Number(ORDER_NO) DOP, LINE_NO DOP_LIN, '0' ORDER_NO, '*' LINE_NO, '*' REL_NO, LINE_ITEM_NO, NULL Cust_part_no," +
                                     " CONTRACT, ORDER_SUPPLY_DEMAND_TYPE, ' ' WRKC, ' ' NEXT_WRKC, PART_NO, STATUS_CODE PART_CODE,DATE_REQUIRED," +
                                     " ifsapp.dop_head_api.Get_Status(ORDER_NO) ORD_STATE, ifsapp.dop_head_api.Get_Due_Date(ORDER_NO) ORD_DATE, " +
                                     "ifsapp.dop_head_api.Get_Qty_Demand(order_no) PROD_QTY, 0 QTY_SUPPLY, QTY_DEMAND, NULL creat_date," +
                                     "owa_opt_lock.checksum(order_no||QTY_DEMAND||DATE_REQUIRED||ORDER_NO||LINE_NO||INFO) chksum  " +
                                     "FROM " +
                                     "ifsapp.dop_order_demand_ext" +
                                        " WHERE part_no = :part_no AND DATE_REQUIRED between :Dat and :Dat1 " +
                                     "UNION ALL " +
                                     "SELECT ifsapp.customer_order_line_api.Get_Pre_Accounting_Id(ORDER_NO, LINE_NO, REL_NO, LINE_ITEM_NO) DOP, '0' DOP_LIN, ORDER_NO, LINE_NO," +
                                     " REL_NO, LINE_ITEM_NO, PART_NO Cust_part_no, CONTRACT, ORDER_SUPPLY_DEMAND_TYPE, ' ' WRKC, ' ' NEXT_WRKC, PART_NO, STATUS_CODE PART_CODE," +
                                     " DATE_REQUIRED,STATUS_CODE ORD_STATE, DATE_REQUIRED ORD_DATE, 0 PROD_QTY, 0 QTY_SUPPLY, QTY_DEMAND," +
                                     " ifsapp.customer_order_line_api.Get_Date_Entered(order_no, line_no, rel_no, line_item_no) creat_date," +
                                     " owa_opt_lock.checksum(ROW_ID||QTY_DEMAND||DATE_REQUIRED||QTY_PEGGED||QTY_RESERVED) chksum " +
                                     "FROM " +
                                        "ifsapp.customer_order_line_demand_oe " +
                                        "WHERE part_no = :part_no AND DATE_REQUIRED between :Dat and :Dat1 " +
                                     "UNION ALL " +
                                     "SELECT 0 DOP, '0' DOP_LIN, a.ORDER_NO, a.LINE_NO, a.REL_NO, a.LINE_ITEM_NO, a.PART_NO Cust_part_no, a.CONTRACT, a.ORDER_SUPPLY_DEMAND_TYPE," +
                                     " ' ' WRKC, ' ' NEXT_WRKC, a.PART_NO, a.STATUS_CODE PART_CODE, a.DATE_REQUIRED, a.STATUS_CODE ORD_STATE, a.DATE_REQUIRED ORD_DATE," +
                                     " 0 PROD_QTY, 0 QTY_SUPPLY, a.QTY_DEMAND, b.DATE_ENTERED creat_date," +
                                     " owa_opt_lock.checksum(a.ROWID||QTY_DEMAND||DATE_REQUIRED||a.STATUS_CODE) chksum  " +
                                     "FROM " +
                                        "ifsapp.material_requis_line_demand_oe a, " +
                                        "ifsapp.material_requis_line b " +
                                        "WHERE b.OBJID = a.ROW_ID and  a.part_no = :part_no AND a.DATE_REQUIRED between :Dat and :Dat1  " +
                                     "UNION ALL  " +
                                     "SELECT 0 DOP, '0' DOP_LIN, ORDER_NO, LINE_NO, REL_NO, LINE_ITEM_NO, PART_NO Cust_part_no, CONTRACT, ORDER_SUPPLY_DEMAND_TYPE," +
                                     " ' ' WRKC, ' ' NEXT_WRKC, PART_NO, STATUS_CODE PART_CODE, DATE_REQUIRED, STATUS_CODE ORD_STATE, DATE_REQUIRED ORD_DATE, 0 PROD_QTY," +
                                     " QTY_SUPPLY, 0 QTY_DEMAND, ifsapp.customer_order_line_api.Get_Date_Entered(order_no, line_no, rel_no, line_item_no) creat_date," +
                                     " owa_opt_lock.checksum(ROWID||QTY_SUPPLY||DATE_REQUIRED) chksum " +
                                     "FROM " +
                                        "ifsapp.purchase_order_line_supply " +
                                        "WHERE part_no = :part_no AND DATE_REQUIRED between :Dat and :Dat1  " +
                                     "UNION ALL SELECT 0 DOP, '0' DOP_LIN, ORDER_NO, LINE_NO, REL_NO, LINE_ITEM_NO, PART_NO Cust_part_no, CONTRACT, ORDER_SUPPLY_DEMAND_TYPE," +
                                     " ' ' WRKC, ' ' NEXT_WRKC, PART_NO, STATUS_CODE PART_CODE, DATE_REQUIRED, STATUS_CODE ORD_STATE, DATE_REQUIRED ORD_DATE, 0 PROD_QTY," +
                                     " QTY_SUPPLY, 0 QTY_DEMAND, ifsapp.customer_order_line_api.Get_Date_Entered(order_no, line_no, rel_no, line_item_no) creat_date," +
                                     " owa_opt_lock.checksum(ROWID||QTY_SUPPLY||DATE_REQUIRED||STATUS_CODE) chksum " +
                                     "FROM " +
                                        "ifsapp.ARRIVED_PUR_ORDER_EXT " +
                                        "WHERE part_no = :part_no AND DATE_REQUIRED between :Dat and :Dat1 ) a " +
                                 "order by to_number(dop) desc,to_number(dop_lin),to_number(int_ord),line_no,rel_no ", conO))
                            {
                                //cmd.FetchSize = cmd.FetchSize * 512;
                                cmd.Parameters.Add(":part_no", OracleDbType.Varchar2);
                                cmd.Parameters.Add(":Dat", OracleDbType.Date);
                                cmd.Parameters.Add(":Dat1", OracleDbType.Date);
                                cmd.Prepare();
                                Log("Prepare command get_ord" + (DateTime.Now - start));
                                using (NpgsqlCommand cmd1 = new NpgsqlCommand("" +
                                    "select * " +
                                    "from public.ord_demands " +
                                    "where part_no=@part_no and DATE_REQUIRED between @first and @last " +
                                    "order by dop desc,dop_lin,int_ord,LINE_NO,REL_NO;", conB))
                                {
                                    cmd1.Parameters.Add("@part_no", NpgsqlTypes.NpgsqlDbType.Varchar);
                                    cmd1.Parameters.Add("@first", NpgsqlTypes.NpgsqlDbType.Date);
                                    cmd1.Parameters.Add("@last", NpgsqlTypes.NpgsqlDbType.Date);
                                    cmd1.Prepare();
                                    DataRow[] dt = MShedul.Select("kol = 0");
                                    if (dt.Length > 0)
                                    {
                                        string part_no = (string)dt[0]["part_no"];
                                        DateTime first = (DateTime)dt[0]["Dat"];
                                        DateTime last = (DateTime)dt[0]["Dat"];
                                        int dt_len = 0;
                                        foreach (DataRow row in dt)
                                        {
                                            if ((string)row["part_no"] != part_no || dt_len == dt.Length - 1)
                                            {
                                                if (first == last) { last = last.AddDays(1); }
                                                cmd.Parameters[0].Value = part_no;
                                                cmd.Parameters[1].Value = first;
                                                cmd.Parameters[2].Value = last;
                                                using (OracleDataReader re = cmd.ExecuteReader())
                                                {
                                                    using (DataTable tmp_rows = new DataTable())
                                                    {
                                                        tmp_rows.Load(re);
                                                        tmp_rows.Columns.Add("id", System.Type.GetType("System.Guid"));
                                                        tmp_rows.Columns.Add("stat", System.Type.GetType("System.String"));

                                                        // Log("read get_ord_ORA for part" + part_no + " Time:" + (DateTime.Now - start));
                                                        cmd1.Parameters[0].Value = part_no;
                                                        cmd1.Parameters[1].Value = first;
                                                        cmd1.Parameters[2].Value = last;
                                                        using (NpgsqlDataReader po1 = cmd1.ExecuteReader())
                                                        {
                                                            using (DataTable sou_rows = new DataTable())
                                                            {
                                                                DataTable sch = po1.GetSchemaTable();
                                                                foreach (DataRow a in sch.Rows)
                                                                {
                                                                    sou_rows.Columns.Add(a["ColumnName"].ToString(), System.Type.GetType(a["DataType"].ToString()));
                                                                    sou_rows.Columns[a["ColumnName"].ToString()].AllowDBNull = true;
                                                                }
                                                                sch.Clear();
                                                                sch.Dispose();
                                                                sou_rows.Load(po1);
                                                                // tmp_rows.DefaultView.Sort = "dop desc,dop_lin,int_ord,line_no,rel_no";
                                                                // Log("read get_ord_postg for part" + part_no + " from:" + first + " To:" + last + " Records ORA:" + tmp_rows.Rows.Count + " Records PSTGR:" + sou_rows.Rows.Count + " Time:" + (DateTime.Now - start));
                                                                int ind_sou_rows = 0;
                                                                int max_sou_rows = sou_rows.Rows.Count;
                                                                using (DataTable era_sourc = sou_rows.Clone())
                                                                {
                                                                    foreach (DataColumn a in era_sourc.Columns)
                                                                    {
                                                                        a.AllowDBNull = true;
                                                                    }
                                                                    int counter = -1;
                                                                    int max = tmp_rows.Rows.Count;
                                                                    foreach (DataRowView rw in tmp_rows.DefaultView)
                                                                    {
                                                                        if (counter < max) { counter++; }
                                                                        if (max_sou_rows > 0 && max_sou_rows > ind_sou_rows)
                                                                        {
                                                                            while (Convert.ToInt32(rw["dop"]) < Convert.ToInt32(sou_rows.DefaultView[ind_sou_rows].Row["dop"]) || (Convert.ToInt32(rw["dop"]) == Convert.ToInt32(sou_rows.DefaultView[ind_sou_rows].Row["dop"]) && (Convert.ToInt32(rw["dop_lin"]) > Convert.ToInt32(sou_rows.DefaultView[ind_sou_rows].Row["dop_lin"]) || (Convert.ToInt32(rw["dop"]) == Convert.ToInt32(sou_rows.DefaultView[ind_sou_rows].Row["dop"]) && (Convert.ToInt32(rw["dop_lin"]) == Convert.ToInt32(sou_rows.DefaultView[ind_sou_rows].Row["dop_lin"]) && Convert.ToInt32(rw["int_ord"]) > Convert.ToInt32(sou_rows.DefaultView[ind_sou_rows].Row["int_ord"]))))))
                                                                            {
                                                                                // zlecenia wcześniejsze do usunięcia
                                                                                DataRow r = era_sourc.NewRow();
                                                                                for (int i = 0; i < sou_rows.Columns.Count; i++)
                                                                                {
                                                                                    r[i] = sou_rows.DefaultView[ind_sou_rows].Row[i];
                                                                                }
                                                                                era_sourc.Rows.Add(r);
                                                                                ind_sou_rows++;
                                                                                if (max_sou_rows <= ind_sou_rows) { break; }
                                                                            }
                                                                            if (max_sou_rows > ind_sou_rows)
                                                                            {
                                                                                if (Convert.ToInt32(rw["dop"]) == Convert.ToInt32(sou_rows.DefaultView[ind_sou_rows].Row["dop"]) && Convert.ToInt32(rw["dop_lin"]) == Convert.ToInt32(sou_rows.DefaultView[ind_sou_rows].Row["dop_lin"]))
                                                                                {
                                                                                    if (Convert.ToInt32(rw["int_ord"]) == Convert.ToInt32(sou_rows.DefaultView[ind_sou_rows].Row["int_ord"]) && (string)rw["line_no"] == (string)sou_rows.DefaultView[ind_sou_rows].Row["line_no"] && (string)rw["rel_no"] == (string)sou_rows.DefaultView[ind_sou_rows].Row["rel_no"])
                                                                                    {
                                                                                        try
                                                                                        {
                                                                                            //rekord istnieje
                                                                                            bool modif = false;
                                                                                            if (Convert.ToInt32(rw["chksum"]) != Convert.ToInt32(sou_rows.DefaultView[ind_sou_rows].Row["chksum"])) { modif = true; }
                                                                                            if (!modif)
                                                                                            {
                                                                                                for (int i = 2; i < tmp_rows.Columns.Count - 2; i++)
                                                                                                {
                                                                                                    bool db_or = false;
                                                                                                    bool db_pst = false;
                                                                                                    if (sou_rows.DefaultView[ind_sou_rows].Row[i] == DBNull.Value || sou_rows.DefaultView[ind_sou_rows].Row[i] == null) { db_pst = true; }
                                                                                                    if (rw[i] == DBNull.Value || rw[i] == null) { db_or = true; }
                                                                                                    if (i == 3 || i == 7 || (i > 17 && i < 21))
                                                                                                    {
                                                                                                        if (Convert.ToDouble(rw[i]) != Convert.ToDouble(sou_rows.DefaultView[ind_sou_rows].Row[i])) { modif = true; }
                                                                                                    }
                                                                                                    else
                                                                                                    {
                                                                                                        if (!db_or && !db_pst)
                                                                                                        {
                                                                                                            if (rw[i].ToString() != sou_rows.DefaultView[ind_sou_rows].Row[i].ToString()) { modif = true; }
                                                                                                        }
                                                                                                        else
                                                                                                        {
                                                                                                            if (db_or != db_pst) { modif = true; }
                                                                                                        }
                                                                                                    }
                                                                                                    if (modif) { break; }
                                                                                                }
                                                                                            }
                                                                                            if (modif)
                                                                                            {
                                                                                                // nastąpiła modyfikacja
                                                                                                rw["stat"] = "MOD";
                                                                                                rw["id"] = sou_rows.DefaultView[ind_sou_rows].Row["id"];
                                                                                            }
                                                                                            ind_sou_rows++;
                                                                                        }
                                                                                        catch (Exception e)
                                                                                        {
                                                                                            Log("Błąd obliczeń podczas sprawdzenia zmian w tabeli Cust_ord:" + e);
                                                                                            return 1;
                                                                                        }
                                                                                    }
                                                                                    else
                                                                                    {
                                                                                        rw["stat"] = "ADD";
                                                                                        rw["id"] = System.Guid.NewGuid();
                                                                                    }
                                                                                }
                                                                                else
                                                                                {
                                                                                    // brak rekordu dodaję
                                                                                    rw["stat"] = "ADD";
                                                                                    rw["id"] = System.Guid.NewGuid();
                                                                                }
                                                                                if (counter >= max - 1 && max_sou_rows > ind_sou_rows)
                                                                                {
                                                                                    while (max_sou_rows > ind_sou_rows)
                                                                                    {
                                                                                        DataRow r = era_sourc.NewRow();
                                                                                        for (int i = 0; i < sou_rows.Columns.Count; i++)
                                                                                        {
                                                                                            r[i] = sou_rows.DefaultView[ind_sou_rows].Row[i];
                                                                                        }
                                                                                        era_sourc.Rows.Add(r);
                                                                                        ind_sou_rows++;
                                                                                        if (max_sou_rows <= ind_sou_rows) { break; }
                                                                                    }
                                                                                }
                                                                            }
                                                                            else
                                                                            {
                                                                                //rekordy do porównania się skończyły
                                                                                rw["stat"] = "ADD";
                                                                            }
                                                                        }
                                                                        else
                                                                        {
                                                                            // nowe dane jeśli brak tabeli PSTGR
                                                                            rw["stat"] = "ADD";
                                                                        }
                                                                    }
                                                                    try
                                                                    {
                                                                        //   Log("END compare get_ord for part" + part_no + " Time:" + (DateTime.Now - start));
                                                                        //Start update
                                                                        DataRow[] rwA = tmp_rows.Select("stat='MOD'");
                                                                        //    Log("RECORDS DOP_ord mod: " + rwA.Length);
                                                                        if (rwA.Length > 0)
                                                                        {
                                                                            using (NpgsqlCommand cmd2 = new NpgsqlCommand("" +
                                                                                "UPDATE public.ord_demands " +
                                                                                "SET dop=@dop, dop_lin=@dop_lin, data_dop=@data_dop, day_shift=@day_shift, order_no=@order_no," +
                                                                                    " line_no=@line_no, rel_no=@rel_no,int_ord=@int_ord, contract=@contract," +
                                                                                    " order_supp_dmd=@order_supp_dmd, wrkc=@wrkc, next_wrkc=@next_wrkc, part_no=@part_no," +
                                                                                    " descr=@descr, part_code=@part_code, date_required=@date_required, ord_state=@ord_state," +
                                                                                    " ord_date=@ord_date, prod_qty=@prod_qty, qty_supply=@qty_supply, qty_demand=@qty_demand," +
                                                                                    " chksum=@chksum ,dat_creat=@dat_creat " +
                                                                                 "where \"id\"=@ID", conB))
                                                                            {
                                                                                cmd2.Parameters.Add("dop", NpgsqlTypes.NpgsqlDbType.Integer);
                                                                                cmd2.Parameters.Add("dop_lin", NpgsqlTypes.NpgsqlDbType.Integer);
                                                                                cmd2.Parameters.Add("data_dop", NpgsqlTypes.NpgsqlDbType.Date);
                                                                                cmd2.Parameters.Add("day_shift", NpgsqlTypes.NpgsqlDbType.Integer);
                                                                                cmd2.Parameters.Add("order_no", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                                                cmd2.Parameters.Add("line_no", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                                                cmd2.Parameters.Add("rel_no", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                                                cmd2.Parameters.Add("int_ord", NpgsqlTypes.NpgsqlDbType.Integer);
                                                                                cmd2.Parameters.Add("contract", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                                                cmd2.Parameters.Add("order_supp_dmd", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                                                cmd2.Parameters.Add("wrkc", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                                                cmd2.Parameters.Add("next_wrkc", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                                                cmd2.Parameters.Add("part_no", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                                                cmd2.Parameters.Add("descr", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                                                cmd2.Parameters.Add("part_code", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                                                cmd2.Parameters.Add("date_required", NpgsqlTypes.NpgsqlDbType.Date);
                                                                                cmd2.Parameters.Add("ord_state", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                                                cmd2.Parameters.Add("ord_date", NpgsqlTypes.NpgsqlDbType.Date);
                                                                                cmd2.Parameters.Add("prod_qty", NpgsqlTypes.NpgsqlDbType.Double);
                                                                                cmd2.Parameters.Add("qty_supply", NpgsqlTypes.NpgsqlDbType.Double);
                                                                                cmd2.Parameters.Add("qty_demand", NpgsqlTypes.NpgsqlDbType.Double);
                                                                                cmd2.Parameters.Add("dat_creat", NpgsqlTypes.NpgsqlDbType.Date);
                                                                                cmd2.Parameters.Add("chksum", NpgsqlTypes.NpgsqlDbType.Integer);
                                                                                cmd2.Parameters.Add("id", NpgsqlTypes.NpgsqlDbType.Uuid);
                                                                                cmd2.Prepare();
                                                                                foreach (DataRow rA in rwA)
                                                                                {
                                                                                    cmd2.Parameters["dop"].Value = Convert.ToInt32(rA["DOP"]);
                                                                                    cmd2.Parameters["dop_lin"].Value = Convert.ToInt32(rA["DOP_LIN"]);
                                                                                    cmd2.Parameters["data_dop"].Value = rA["DATA_DOP"] ?? DBNull.Value;
                                                                                    cmd2.Parameters["day_shift"].Value = Convert.ToInt32(rA["DAY_SHIFT"]);
                                                                                    cmd2.Parameters["order_no"].Value = (string)rA["ORDER_NO"];
                                                                                    cmd2.Parameters["line_no"].Value = (string)rA["LINE_NO"];
                                                                                    cmd2.Parameters["rel_no"].Value = (string)rA["REL_NO"];
                                                                                    cmd2.Parameters["int_ord"].Value = Convert.ToInt32(rA["INT_ORD"]);
                                                                                    cmd2.Parameters["contract"].Value = (string)rA["CONTRACT"];
                                                                                    cmd2.Parameters["order_supp_dmd"].Value = (string)rA["ORDER_SUPPLY_DEMAND_TYPE"];
                                                                                    cmd2.Parameters["wrkc"].Value = (string)rA["WRKC"];
                                                                                    cmd2.Parameters["next_wrkc"].Value = (string)rA["NEXT_WRKC"];
                                                                                    cmd2.Parameters["part_no"].Value = (string)rA["PART_NO"];
                                                                                    cmd2.Parameters["descr"].Value = (string)rA["DESCR"];
                                                                                    cmd2.Parameters["part_code"].Value = (string)rA["PART_CODE"];
                                                                                    cmd2.Parameters["date_required"].Value = rA["DATE_REQUIRED"] ?? DBNull.Value;
                                                                                    cmd2.Parameters["ord_state"].Value = (string)rA["ORD_STATE"];
                                                                                    cmd2.Parameters["ord_date"].Value = rA["ORD_DATE"] ?? DBNull.Value;
                                                                                    cmd2.Parameters["prod_qty"].Value = Convert.ToDouble(rA["PROD_QTY"]);
                                                                                    cmd2.Parameters["qty_supply"].Value = Convert.ToDouble(rA["QTY_SUPPLY"]);
                                                                                    cmd2.Parameters["qty_demand"].Value = Convert.ToDouble(rA["QTY_DEMAND"]);
                                                                                    cmd2.Parameters["dat_creat"].Value = rA["CREAT_DATE"] ?? DBNull.Value;
                                                                                    cmd2.Parameters["chksum"].Value = Convert.ToInt32(rA["CHKSUM"]);
                                                                                    cmd2.Parameters["id"].Value = (Guid)rA["ID"];
                                                                                    cmd2.ExecuteNonQuery();
                                                                                }
                                                                            }

                                                                        }
                                                                        // DODAJ dane
                                                                        rwA = tmp_rows.Select("stat='ADD'");
                                                                        //  Log("RECORDS DOP_ord add: " + rwA.Length);
                                                                        if (rwA.Length > 0)
                                                                        {
                                                                            using (NpgsqlCommand cmd2 = new NpgsqlCommand("" +
                                                                                "INSERT INTO public.ord_demands" +
                                                                                    "(dop, dop_lin, data_dop, day_shift, order_no, line_no, rel_no, int_ord ,contract, order_supp_dmd, wrkc," +
                                                                                    " next_wrkc, part_no, descr, part_code, date_required, ord_state, ord_date, prod_qty, qty_supply," +
                                                                                    " qty_demand,dat_creat , chksum, id) " +
                                                                                "VALUES " +
                                                                                    "(@dop,@dop_lin,@data_dop,@day_shift,@order_no,@line_no,@rel_no,@int_ord,@contract,@order_supp_dmd," +
                                                                                    "@wrkc,@next_wrkc,@part_no,@descr,@part_code,@date_required,@ord_state,@ord_date,@prod_qty," +
                                                                                    "@qty_supply,@qty_demand,@dat_creat,@chksum,@id);", conB))
                                                                            {
                                                                                cmd2.Parameters.Add("dop", NpgsqlTypes.NpgsqlDbType.Integer);
                                                                                cmd2.Parameters.Add("dop_lin", NpgsqlTypes.NpgsqlDbType.Integer);
                                                                                cmd2.Parameters.Add("data_dop", NpgsqlTypes.NpgsqlDbType.Date);
                                                                                cmd2.Parameters.Add("day_shift", NpgsqlTypes.NpgsqlDbType.Integer);
                                                                                cmd2.Parameters.Add("order_no", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                                                cmd2.Parameters.Add("line_no", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                                                cmd2.Parameters.Add("rel_no", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                                                cmd2.Parameters.Add("int_ord", NpgsqlTypes.NpgsqlDbType.Integer);
                                                                                cmd2.Parameters.Add("contract", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                                                cmd2.Parameters.Add("order_supp_dmd", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                                                cmd2.Parameters.Add("wrkc", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                                                cmd2.Parameters.Add("next_wrkc", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                                                cmd2.Parameters.Add("part_no", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                                                cmd2.Parameters.Add("descr", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                                                cmd2.Parameters.Add("part_code", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                                                cmd2.Parameters.Add("date_required", NpgsqlTypes.NpgsqlDbType.Date);
                                                                                cmd2.Parameters.Add("ord_state", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                                                cmd2.Parameters.Add("ord_date", NpgsqlTypes.NpgsqlDbType.Date);
                                                                                cmd2.Parameters.Add("prod_qty", NpgsqlTypes.NpgsqlDbType.Double);
                                                                                cmd2.Parameters.Add("qty_supply", NpgsqlTypes.NpgsqlDbType.Double);
                                                                                cmd2.Parameters.Add("qty_demand", NpgsqlTypes.NpgsqlDbType.Double);
                                                                                cmd2.Parameters.Add("dat_creat", NpgsqlTypes.NpgsqlDbType.Date);
                                                                                cmd2.Parameters.Add("chksum", NpgsqlTypes.NpgsqlDbType.Integer);
                                                                                cmd2.Parameters.Add("id", NpgsqlTypes.NpgsqlDbType.Uuid);
                                                                                cmd2.Prepare();
                                                                                foreach (DataRow rA in rwA)
                                                                                {
                                                                                    cmd2.Parameters["dop"].Value = Convert.ToInt32(rA["DOP"]);
                                                                                    cmd2.Parameters["dop_lin"].Value = Convert.ToInt32(rA["DOP_LIN"]);
                                                                                    cmd2.Parameters["data_dop"].Value = rA["DATA_DOP"] ?? DBNull.Value;
                                                                                    cmd2.Parameters["day_shift"].Value = Convert.ToInt32(rA["DAY_SHIFT"]);
                                                                                    cmd2.Parameters["order_no"].Value = (string)rA["ORDER_NO"];
                                                                                    cmd2.Parameters["line_no"].Value = (string)rA["LINE_NO"];
                                                                                    cmd2.Parameters["rel_no"].Value = (string)rA["REL_NO"];
                                                                                    cmd2.Parameters["int_ord"].Value = Convert.ToInt32(rA["INT_ORD"]);
                                                                                    cmd2.Parameters["contract"].Value = (string)rA["CONTRACT"];
                                                                                    cmd2.Parameters["order_supp_dmd"].Value = (string)rA["ORDER_SUPPLY_DEMAND_TYPE"];
                                                                                    cmd2.Parameters["wrkc"].Value = (string)rA["WRKC"];
                                                                                    cmd2.Parameters["next_wrkc"].Value = (string)rA["NEXT_WRKC"];
                                                                                    cmd2.Parameters["part_no"].Value = (string)rA["PART_NO"];
                                                                                    cmd2.Parameters["descr"].Value = (string)rA["DESCR"];
                                                                                    cmd2.Parameters["part_code"].Value = (string)rA["PART_CODE"];
                                                                                    cmd2.Parameters["date_required"].Value = rA["DATE_REQUIRED"] ?? DBNull.Value;
                                                                                    cmd2.Parameters["ord_state"].Value = (string)rA["ORD_STATE"];
                                                                                    cmd2.Parameters["ord_date"].Value = rA["ORD_DATE"] ?? DBNull.Value;
                                                                                    cmd2.Parameters["prod_qty"].Value = Convert.ToDouble(rA["PROD_QTY"]);
                                                                                    cmd2.Parameters["qty_supply"].Value = Convert.ToDouble(rA["QTY_SUPPLY"]);
                                                                                    cmd2.Parameters["qty_demand"].Value = Convert.ToDouble(rA["QTY_DEMAND"]);
                                                                                    cmd2.Parameters["dat_creat"].Value = rA["CREAT_DATE"] ?? DBNull.Value;
                                                                                    cmd2.Parameters["chksum"].Value = Convert.ToInt32(rA["CHKSUM"]);
                                                                                    cmd2.Parameters["id"].Value = System.Guid.NewGuid();
                                                                                    cmd2.ExecuteNonQuery();
                                                                                }
                                                                                //    Log("END ADD DOP_ord for part:" + (DateTime.Now - start));
                                                                            }
                                                                        }
                                                                        if (era_sourc.Rows.Count > 0)
                                                                        {
                                                                            using (NpgsqlCommand cmd2 = new NpgsqlCommand("" +
                                                                                "delete from ord_demands " +
                                                                                "where \"id\"=@id", conB))
                                                                            {
                                                                                cmd2.Parameters.Add("@id", NpgsqlTypes.NpgsqlDbType.Uuid);
                                                                                cmd2.Prepare();
                                                                                foreach (DataRowView rA in era_sourc.DefaultView)
                                                                                {
                                                                                    cmd2.Parameters[0].Value = rA["id"];
                                                                                    cmd2.ExecuteNonQuery();
                                                                                }
                                                                                //    Log("ERASE dont match DOP_ord for part" + (DateTime.Now - start));
                                                                            }
                                                                        }
                                                                        //Log("END MODIFY DOP_ord for part:" + (DateTime.Now - start));

                                                                    }
                                                                    catch (Exception e)
                                                                    {
                                                                        Log("Błąd modyfikacji tabeli ord_dmd postegrsql:" + e);
                                                                    }
                                                                }
                                                            }
                                                        }
                                                        //   Log("read get_ord for part" + part_no + " Time:" + (DateTime.Now - start));
                                                    }
                                                }
                                                part_no = (string)row["part_no"];
                                                first = (DateTime)row["Dat"];
                                                last = (DateTime)row["Dat"];
                                            }
                                            last = (DateTime)row["Dat"];
                                        }
                                    }
                                }
                                using (NpgsqlCommand cmd1 = new NpgsqlCommand("" +
                                    "UPDATE public.datatbles " +
                                    "SET last_modify=current_timestamp, in_progress=false,updt_errors=false " +
                                    "WHERE table_name='worker1'", conB))
                                {
                                    cmd1.ExecuteNonQuery();
                                }
                            }
                            await TR_updt.CommitAsync();
                        }
                    }

                }
                GC.Collect();
                Log("End WORKER1");
                return 0;
            }
            catch (Exception e)
            {
                Log("Błąd odczytu informacji z ORACLE :" + e);
                return 1;
            }

        }
        private async Task<int> Get_ord_dem1()
        {
            try
            {
                using (OracleConnection conO = new OracleConnection("Password = pass;User ID = user; Data Source = prod8"))
                {
                    await conO.OpenAsync();
                    using (NpgsqlConnection conB = new NpgsqlConnection(npA))
                    {
                        conB.Open();
                        using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                            "UPDATE public.datatbles " +
                            "SET start_update=current_timestamp, in_progress=true " +
                            "WHERE table_name='worker2'", conB))
                        {
                            cmd.ExecuteNonQuery();
                        }
                        Log("start get_ord1:" + (DateTime.Now - start));
                        using (OracleCommand cmd = new OracleCommand("" +
                            "SELECT  a.DOP,Nvl(a.LINE_ITEM_NO,a.DOP_LIN) DOP_LIN,Nvl(ifsapp.dop_order_api.Get_Revised_Due_Date(a.DOP,1),a.DATE_REQUIRED) Data_dop," +
                            "Nvl(ifsapp.work_time_calendar_api.Get_Work_Days_Between(ifsapp.site_api.Get_Manuf_Calendar_Id(a.CONTRACT), " +
                                "ifsapp.dop_order_api.Get_Revised_Start_Date(a.DOP, a.DOP_LIN), " +
                                "ifsapp.dop_order_api.Get_Revised_Due_Date(a.DOP, 1)) + ifsapp.dop_order_api.Dop_Order_Slack(a.DOP, a.DOP_LIN) + 1, 0) DAy_shift," +
                            "a.ORDER_NO, a.LINE_NO, a.REL_NO,cast(Decode(LENGTH(TRIM(TRANSLATE(a.ORDER_NO, ' +-.0123456789',' '))),NULL,a.ORDER_NO," +
                                "owa_opt_lock.checksum(a.ORDER_NO)*-1) as INT) int_ord," +
                            " a.CONTRACT, a.ORDER_SUPPLY_DEMAND_TYPE, a.WRKC, a.NEXT_WRKC, a.PART_NO, ifsapp.inventory_part_api.Get_Description(CONTRACT, a.PART_NO) Descr," +
                            " a.PART_CODE, a.DATE_REQUIRED,a.ORD_STATE, a.ORD_DATE, cast(a.PROD_QTY as float) PROD_QTY, cast(a.QTY_SUPPLY as float) QTY_SUPPLY, " +
                            "cast(a.QTY_DEMAND as float) QTY_DEMAND,To_Date(creat_date) creat_date, chksum " +
                            "from " +
                                 "(SELECT Nvl(ifsapp.dop_supply_shop_ord_api.Get_C_Dop_Id(order_no, line_no, rel_no), 0) DOP," +
                                 "Nvl(REPLACE(SubStr(ifsapp.shop_ord_api.Get_Source(order_no, line_no, rel_no)," +
                                    " InStr(ifsapp.shop_ord_api.Get_Source(order_no, line_no, rel_no), '^', 10)), '^', ''), 0) DOP_LIN," +
                                 "order_no, line_no, rel_no, LINE_ITEM_NO, ifsapp.shop_ord_api.Get_Part_No(order_no, line_no, rel_no) Cust_part_no, CONTRACT," +
                                 " ORDER_SUPPLY_DEMAND_TYPE,ifsapp.shop_order_operation_list_api.Get_Next_Op_Work_Center(order_no, line_no, rel_no," +
                                    " ifsapp.shop_order_operation_list_api.Get_Prev_Non_Parallel_Op(order_no, line_no, rel_no, 1, 0)) WRKC," +
                                 "ifsapp.shop_order_operation_list_api.Get_Next_Op_Work_Center(order_no, line_no, rel_no, " +
                                    "ifsapp.shop_order_operation_list_api.Get_Prev_Non_Parallel_Op(order_no, line_no, rel_no, 2, 0)) NEXT_WRKC," +
                                 "PART_NO, STATUS_CODE PART_CODE, DATE_REQUIRED, ifsapp.shop_ord_api.Get_State(order_no, line_no, rel_no) ORD_STATE," +
                                 " ifsapp.shop_ord_api.Get_Revised_Due_Date(order_no, line_no, rel_no) ORD_date," +
                                 "Nvl(ifsapp.shop_ord_api.Get_Revised_Qty_Due(order_no, line_no, rel_no)-ifsapp.shop_ord_api.Get_Qty_Complete(order_no, line_no, rel_no), 0) Prod_QTY," +
                                 "0 QTY_SUPPLY, QTY_DEMAND, ifsapp.shop_ord_api.Get_Date_Entered(order_no, line_no, rel_no) creat_date," +
                                 " owa_opt_lock.checksum(ROWID||QTY_DEMAND||QTY_PEGGED||QTY_RESERVED||To_Char(ifsapp.shop_order_operation_api.Get_Op_Start_Date(order_no,line_no,rel_no," +
                                    "ifsapp.shop_order_operation_list_api.Get_Prev_Non_Parallel_Op (order_no,line_no,rel_no,2,0)),'YYYYMMDDHH24miss')) chksum" +
                                    "  FROM " +
                                        "ifsapp.shop_material_alloc_demand WHERE part_no = :part_no AND DATE_REQUIRED between :Dat and :Dat1 " +
                                        "UNION ALL " +
                                        "sELECT To_Number(ORDER_NO) DOP, LINE_NO DOP_LIN, '0' ORDER_NO, '*' LINE_NO, '*' REL_NO, LINE_ITEM_NO, NULL Cust_part_no, CONTRACT, ORDER_SUPPLY_DEMAND_TYPE, ' ' WRKC, ' ' NEXT_WRKC, " +
                                        "PART_NO, STATUS_CODE PART_CODE,DATE_REQUIRED, ifsapp.dop_head_api.Get_Status(ORDER_NO) ORD_STATE, ifsapp.dop_head_api.Get_Due_Date(ORDER_NO) ORD_DATE, " +
                                        "ifsapp.dop_head_api.Get_Qty_Demand(order_no) PROD_QTY, 0 QTY_SUPPLY, QTY_DEMAND, " +
                                        "NULL creat_date,owa_opt_lock.checksum(order_no||QTY_DEMAND||DATE_REQUIRED||ORDER_NO||LINE_NO||INFO) chksum  " +
                                        "FROM " +
                                        "ifsapp.dop_order_demand_ext " +
                                        "WHERE part_no = :part_no AND DATE_REQUIRED between :Dat and :Dat1 " +
                                        "UNION ALL " +
                                        "SELECT ifsapp.customer_order_line_api.Get_Pre_Accounting_Id(ORDER_NO, LINE_NO, REL_NO, LINE_ITEM_NO) DOP, '0' DOP_LIN, ORDER_NO, LINE_NO, REL_NO, LINE_ITEM_NO, " +
                                        "PART_NO Cust_part_no, CONTRACT, ORDER_SUPPLY_DEMAND_TYPE, ' ' WRKC, ' ' NEXT_WRKC, PART_NO, STATUS_CODE PART_CODE, DATE_REQUIRED,STATUS_CODE ORD_STATE, " +
                                        "DATE_REQUIRED ORD_DATE, 0 PROD_QTY, 0 QTY_SUPPLY, QTY_DEMAND, ifsapp.customer_order_line_api.Get_Date_Entered(order_no, line_no, rel_no, line_item_no) creat_date, " +
                                        "owa_opt_lock.checksum(ROW_ID||QTY_DEMAND||DATE_REQUIRED||QTY_PEGGED||QTY_RESERVED) chksum " +
                                        "FROM " +
                                        "ifsapp.customer_order_line_demand_oe " +
                                        "WHERE part_no = :part_no AND DATE_REQUIRED between :Dat and :Dat1 " +
                                        "UNION ALL " +
                                        "SELECT 0 DOP, '0' DOP_LIN, a.ORDER_NO, a.LINE_NO, a.REL_NO, a.LINE_ITEM_NO, a.PART_NO Cust_part_no, a.CONTRACT, a.ORDER_SUPPLY_DEMAND_TYPE, ' ' WRKC, ' ' NEXT_WRKC, a.PART_NO, " +
                                        "a.STATUS_CODE PART_CODE, a.DATE_REQUIRED, a.STATUS_CODE ORD_STATE, a.DATE_REQUIRED ORD_DATE, 0 PROD_QTY, 0 QTY_SUPPLY, a.QTY_DEMAND, b.DATE_ENTERED creat_date, " +
                                        "owa_opt_lock.checksum(a.ROWID||QTY_DEMAND||DATE_REQUIRED||a.STATUS_CODE) chksum  " +
                                        "FROM ifsapp.material_requis_line_demand_oe a, " +
                                        "ifsapp.material_requis_line b " +
                                        "WHERE b.OBJID = a.ROW_ID and  a.part_no = :part_no AND a.DATE_REQUIRED between :Dat and :Dat1  " +
                                        "UNION ALL  " +
                                        "SELECT 0 DOP, '0' DOP_LIN, ORDER_NO, LINE_NO, REL_NO, LINE_ITEM_NO, PART_NO Cust_part_no, CONTRACT, ORDER_SUPPLY_DEMAND_TYPE, ' ' WRKC, ' ' NEXT_WRKC, PART_NO, " +
                                        "STATUS_CODE PART_CODE, DATE_REQUIRED, STATUS_CODE ORD_STATE, DATE_REQUIRED ORD_DATE, 0 PROD_QTY, QTY_SUPPLY, 0 QTY_DEMAND, " +
                                        "ifsapp.customer_order_line_api.Get_Date_Entered(order_no, line_no, rel_no, line_item_no) creat_date, owa_opt_lock.checksum(ROWID||QTY_SUPPLY||DATE_REQUIRED) chksum " +
                                        "FROM ifsapp.purchase_order_line_supply " +
                                        "WHERE part_no = :part_no AND DATE_REQUIRED between :Dat and :Dat1  " +
                                        "UNION ALL " +
                                        "SELECT 0 DOP, '0' DOP_LIN, ORDER_NO, LINE_NO, REL_NO, LINE_ITEM_NO, PART_NO Cust_part_no, CONTRACT, ORDER_SUPPLY_DEMAND_TYPE, ' ' WRKC, ' ' NEXT_WRKC, PART_NO, " +
                                        "STATUS_CODE PART_CODE, DATE_REQUIRED, STATUS_CODE ORD_STATE, DATE_REQUIRED ORD_DATE, 0 PROD_QTY, QTY_SUPPLY, 0 QTY_DEMAND, " +
                                        "ifsapp.customer_order_line_api.Get_Date_Entered(order_no, line_no, rel_no, line_item_no) creat_date, owa_opt_lock.checksum(ROWID||QTY_SUPPLY||DATE_REQUIRED||STATUS_CODE) chksum " +
                                        "FROM " +
                                        "ifsapp.ARRIVED_PUR_ORDER_EXT " +
                                        "WHERE part_no = :part_no AND DATE_REQUIRED between :Dat and :Dat1 ) a " +
                                     "order by to_number(dop) desc,to_number(dop_lin),to_number(int_ord),line_no,rel_no  ", conO))
                        {
                            //cmd.FetchSize = cmd.FetchSize * 512;
                            cmd.Parameters.Add(":part_no", OracleDbType.Varchar2);
                            cmd.Parameters.Add(":Dat", OracleDbType.Date);
                            cmd.Parameters.Add(":Dat1", OracleDbType.Date);
                            cmd.Prepare();
                            Log("Prepare command get_ord1:" + (DateTime.Now - start));
                            using (NpgsqlCommand cmd1 = new NpgsqlCommand("" +
                                "select * from public.ord_demands " +
                                "where part_no=@part_no and DATE_REQUIRED between @first and @last " +
                                "order by dop desc,dop_lin,int_ord,LINE_NO,REL_NO;", conB))
                            {
                                cmd1.Parameters.Add("@part_no", NpgsqlTypes.NpgsqlDbType.Varchar);
                                cmd1.Parameters.Add("@first", NpgsqlTypes.NpgsqlDbType.Date);
                                cmd1.Parameters.Add("@last", NpgsqlTypes.NpgsqlDbType.Date);
                                cmd1.Prepare();
                                DataRow[] dt = MShedul.Select("kol = 1");
                                if (dt.Length > 0)
                                {
                                    string part_no = (string)dt[0]["part_no"];
                                    DateTime first = (DateTime)dt[0]["Dat"];
                                    DateTime last = (DateTime)dt[0]["Dat"];
                                    int bgc = 0; int dt_len = 0;
                                    foreach (DataRow row in dt)
                                    {
                                        bgc++;
                                        if ((string)row["part_no"] != part_no || dt_len == dt.Length - 1)
                                        {
                                            if (first == last) { last = last.AddDays(1); }
                                            cmd.Parameters[0].Value = part_no;
                                            cmd.Parameters[1].Value = first;
                                            cmd.Parameters[2].Value = last;
                                            using (OracleDataReader re = cmd.ExecuteReader())
                                            {
                                                using (DataTable tmp_rows = new DataTable())
                                                {
                                                    tmp_rows.Load(re);
                                                    tmp_rows.Columns.Add("id", System.Type.GetType("System.Guid"));
                                                    tmp_rows.Columns.Add("stat", System.Type.GetType("System.String"));

                                                    //   Log("read get_ord_ORA for part1:" + part_no + " Time:" + (DateTime.Now - start));
                                                    cmd1.Parameters[0].Value = part_no;
                                                    cmd1.Parameters[1].Value = first;
                                                    cmd1.Parameters[2].Value = last;
                                                    using (NpgsqlDataReader po1 = cmd1.ExecuteReader())
                                                    {

                                                        using (DataTable sou_rows = new DataTable())
                                                        {
                                                            DataTable sch = po1.GetSchemaTable();
                                                            foreach (DataRow a in sch.Rows)
                                                            {
                                                                sou_rows.Columns.Add(a["ColumnName"].ToString(), System.Type.GetType(a["DataType"].ToString()));
                                                                sou_rows.Columns[a["ColumnName"].ToString()].AllowDBNull = true;
                                                            }
                                                            sch.Clear();
                                                            sch.Dispose();
                                                            sou_rows.Load(po1);
                                                            //tmp_rows.DefaultView.Sort = "dop desc,dop_lin,int_ord,LINE_NO,REL_NO";
                                                            //     Log("read get_ord_postg for part1:" + part_no + " from:" + first + " To:" + last + " Records ORA:" + tmp_rows.Rows.Count + " Records PSTGR:" + sou_rows.Rows.Count + " Time:" + (DateTime.Now - start));
                                                            int ind_sou_rows = 0;
                                                            int max_sou_rows = sou_rows.Rows.Count;
                                                            using (DataTable era_sourc = sou_rows.Clone())
                                                            {
                                                                foreach (DataColumn a in era_sourc.Columns)
                                                                {
                                                                    a.AllowDBNull = true;
                                                                }
                                                                int counter = -1;
                                                                int max = tmp_rows.Rows.Count;
                                                                foreach (DataRowView rw in tmp_rows.DefaultView)
                                                                {
                                                                    if (counter < max) { counter++; }
                                                                    if (sou_rows.Rows.Count > 0 && max_sou_rows > ind_sou_rows)
                                                                    {
                                                                        while (Convert.ToInt32(rw["dop"]) < Convert.ToInt32(sou_rows.DefaultView[ind_sou_rows].Row["dop"]) || (Convert.ToInt32(rw["dop"]) == Convert.ToInt32(sou_rows.DefaultView[ind_sou_rows].Row["dop"]) && (Convert.ToInt32(rw["dop_lin"]) > Convert.ToInt32(sou_rows.DefaultView[ind_sou_rows].Row["dop_lin"]) || (Convert.ToInt32(rw["dop"]) == Convert.ToInt32(sou_rows.DefaultView[ind_sou_rows].Row["dop"]) && (Convert.ToInt32(rw["dop_lin"]) == Convert.ToInt32(sou_rows.DefaultView[ind_sou_rows].Row["dop_lin"]) && Convert.ToInt32(rw["int_ord"]) > Convert.ToInt32(sou_rows.DefaultView[ind_sou_rows].Row["int_ord"]))))))
                                                                        {
                                                                            // zlecenia wcześniejsze do usunięcia
                                                                            DataRow r = era_sourc.NewRow();
                                                                            for (int i = 0; i < sou_rows.Columns.Count; i++)
                                                                            {
                                                                                r[i] = sou_rows.DefaultView[ind_sou_rows].Row[i];
                                                                            }
                                                                            era_sourc.Rows.Add(r);
                                                                            ind_sou_rows++;
                                                                            if (max_sou_rows <= ind_sou_rows) { break; }
                                                                        }
                                                                        if (max_sou_rows > ind_sou_rows)
                                                                        {
                                                                            if (Convert.ToInt32(rw["dop"]) == Convert.ToInt32(sou_rows.DefaultView[ind_sou_rows].Row["dop"]) && Convert.ToInt32(rw["dop_lin"]) == Convert.ToInt32(sou_rows.DefaultView[ind_sou_rows].Row["dop_lin"]))
                                                                            {
                                                                                if (Convert.ToInt32(rw["int_ord"]) == Convert.ToInt32(sou_rows.DefaultView[ind_sou_rows].Row["int_ord"]) && (string)rw["line_no"] == (string)sou_rows.DefaultView[ind_sou_rows].Row["line_no"] && (string)rw["rel_no"] == (string)sou_rows.DefaultView[ind_sou_rows].Row["rel_no"])
                                                                                {
                                                                                    //rekord istnieje
                                                                                    bool modif = false;
                                                                                    if (Convert.ToInt32(rw["chksum"]) != Convert.ToInt32(sou_rows.DefaultView[ind_sou_rows].Row["chksum"])) { modif = true; }
                                                                                    if (!modif)
                                                                                    {
                                                                                        for (int i = 2; i < tmp_rows.Columns.Count - 2; i++)
                                                                                        {
                                                                                            bool db_or = false;
                                                                                            bool db_pst = false;
                                                                                            if (sou_rows.DefaultView[ind_sou_rows].Row[i] == DBNull.Value || sou_rows.DefaultView[ind_sou_rows].Row[i] == null) { db_pst = true; }
                                                                                            if (rw[i] == DBNull.Value || rw[i] == null) { db_or = true; }
                                                                                            if (i == 3 || i == 7 || (i > 17 && i < 21))
                                                                                            {
                                                                                                if (Convert.ToDouble(rw[i]) != Convert.ToDouble(sou_rows.DefaultView[ind_sou_rows].Row[i])) { modif = true; }
                                                                                            }
                                                                                            else
                                                                                            {
                                                                                                if (!db_or && !db_pst)
                                                                                                {
                                                                                                    if (rw[i].ToString() != sou_rows.DefaultView[ind_sou_rows].Row[i].ToString()) { modif = true; }
                                                                                                }
                                                                                                else
                                                                                                {
                                                                                                    if (db_or != db_pst) { modif = true; }
                                                                                                }
                                                                                            }
                                                                                            if (modif) { break; }
                                                                                        }
                                                                                    }
                                                                                    if (modif)
                                                                                    {
                                                                                        // nastąpiła modyfikacja
                                                                                        rw["stat"] = "MOD";
                                                                                        rw["id"] = sou_rows.DefaultView[ind_sou_rows].Row["id"];
                                                                                    }
                                                                                    ind_sou_rows++;
                                                                                }
                                                                                else
                                                                                {
                                                                                    // brak rekordu dodaję
                                                                                    rw["stat"] = "ADD";
                                                                                    rw["id"] = System.Guid.NewGuid();
                                                                                }
                                                                            }
                                                                            else
                                                                            {
                                                                                // brak rekordu dodaję
                                                                                rw["stat"] = "ADD";
                                                                                rw["id"] = System.Guid.NewGuid();
                                                                            }
                                                                            if (counter >= max - 1 && max_sou_rows > ind_sou_rows)
                                                                            {
                                                                                while (max_sou_rows > ind_sou_rows)
                                                                                {
                                                                                    DataRow r = era_sourc.NewRow();
                                                                                    for (int i = 0; i < sou_rows.Columns.Count; i++)
                                                                                    {
                                                                                        r[i] = sou_rows.DefaultView[ind_sou_rows].Row[i];
                                                                                    }
                                                                                    era_sourc.Rows.Add(r);
                                                                                    ind_sou_rows++;
                                                                                    if (max_sou_rows <= ind_sou_rows) { break; }
                                                                                }
                                                                            }
                                                                        }
                                                                        else
                                                                        {
                                                                            //rekordy do porównania się skończyły
                                                                            rw["stat"] = "ADD";
                                                                        }
                                                                    }
                                                                    else
                                                                    {
                                                                        // nowe dane jeśli brak tabeli PSTGR
                                                                        rw["stat"] = "ADD";
                                                                    }
                                                                }
                                                                //     Log("END compare get_ord for part1:" + part_no + " Time:" + (DateTime.Now - start));
                                                                //Start update
                                                                try
                                                                {
                                                                    DataRow[] rwA = tmp_rows.Select("stat='MOD'");
                                                                    // Log("RECORDS DOP_ord mod1: " + rwA.Length);
                                                                    if (rwA.Length > 0)
                                                                    {
                                                                        using (NpgsqlTransaction TR_updt = conB.BeginTransaction())
                                                                        {
                                                                            using (NpgsqlCommand cmd2 = new NpgsqlCommand("" +
                                                                                "UPDATE public.ord_demands " +
                                                                                "SET dop=@dop, dop_lin=@dop_lin, data_dop=@data_dop, day_shift=@day_shift, order_no=@order_no, line_no=@line_no, rel_no=@rel_no," +
                                                                                "int_ord=@int_ord, contract=@contract, order_supp_dmd=@order_supp_dmd, wrkc=@wrkc, next_wrkc=@next_wrkc, part_no=@part_no, descr=@descr," +
                                                                                " part_code=@part_code, date_required=@date_required, ord_state=@ord_state, ord_date=@ord_date, prod_qty=@prod_qty, qty_supply=@qty_supply," +
                                                                                " qty_demand=@qty_demand, chksum=@chksum ,dat_creat=@dat_creat where \"id\"=@ID", conB))
                                                                            {
                                                                                cmd2.Parameters.Add("dop", NpgsqlTypes.NpgsqlDbType.Integer);
                                                                                cmd2.Parameters.Add("dop_lin", NpgsqlTypes.NpgsqlDbType.Integer);
                                                                                cmd2.Parameters.Add("data_dop", NpgsqlTypes.NpgsqlDbType.Date);
                                                                                cmd2.Parameters.Add("day_shift", NpgsqlTypes.NpgsqlDbType.Integer);
                                                                                cmd2.Parameters.Add("order_no", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                                                cmd2.Parameters.Add("line_no", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                                                cmd2.Parameters.Add("rel_no", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                                                cmd2.Parameters.Add("int_ord", NpgsqlTypes.NpgsqlDbType.Integer);
                                                                                cmd2.Parameters.Add("contract", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                                                cmd2.Parameters.Add("order_supp_dmd", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                                                cmd2.Parameters.Add("wrkc", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                                                cmd2.Parameters.Add("next_wrkc", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                                                cmd2.Parameters.Add("part_no", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                                                cmd2.Parameters.Add("descr", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                                                cmd2.Parameters.Add("part_code", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                                                cmd2.Parameters.Add("date_required", NpgsqlTypes.NpgsqlDbType.Date);
                                                                                cmd2.Parameters.Add("ord_state", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                                                cmd2.Parameters.Add("ord_date", NpgsqlTypes.NpgsqlDbType.Date);
                                                                                cmd2.Parameters.Add("prod_qty", NpgsqlTypes.NpgsqlDbType.Double);
                                                                                cmd2.Parameters.Add("qty_supply", NpgsqlTypes.NpgsqlDbType.Double);
                                                                                cmd2.Parameters.Add("qty_demand", NpgsqlTypes.NpgsqlDbType.Double);
                                                                                cmd2.Parameters.Add("dat_creat", NpgsqlTypes.NpgsqlDbType.Date);
                                                                                cmd2.Parameters.Add("chksum", NpgsqlTypes.NpgsqlDbType.Integer);
                                                                                cmd2.Parameters.Add("id", NpgsqlTypes.NpgsqlDbType.Uuid);
                                                                                cmd2.Prepare();
                                                                                foreach (DataRow rA in rwA)
                                                                                {
                                                                                    cmd2.Parameters["dop"].Value = Convert.ToInt32(rA["DOP"]);
                                                                                    cmd2.Parameters["dop_lin"].Value = Convert.ToInt32(rA["DOP_LIN"]);
                                                                                    cmd2.Parameters["data_dop"].Value = rA["DATA_DOP"] ?? DBNull.Value;
                                                                                    cmd2.Parameters["day_shift"].Value = Convert.ToInt32(rA["DAY_SHIFT"]);
                                                                                    cmd2.Parameters["order_no"].Value = (string)rA["ORDER_NO"];
                                                                                    cmd2.Parameters["line_no"].Value = (string)rA["LINE_NO"];
                                                                                    cmd2.Parameters["rel_no"].Value = (string)rA["REL_NO"];
                                                                                    cmd2.Parameters["int_ord"].Value = Convert.ToInt32(rA["INT_ORD"]);
                                                                                    cmd2.Parameters["contract"].Value = (string)rA["CONTRACT"];
                                                                                    cmd2.Parameters["order_supp_dmd"].Value = (string)rA["ORDER_SUPPLY_DEMAND_TYPE"];
                                                                                    cmd2.Parameters["wrkc"].Value = (string)rA["WRKC"];
                                                                                    cmd2.Parameters["next_wrkc"].Value = (string)rA["NEXT_WRKC"];
                                                                                    cmd2.Parameters["part_no"].Value = (string)rA["PART_NO"];
                                                                                    cmd2.Parameters["descr"].Value = (string)rA["DESCR"];
                                                                                    cmd2.Parameters["part_code"].Value = (string)rA["PART_CODE"];
                                                                                    cmd2.Parameters["date_required"].Value = rA["DATE_REQUIRED"] ?? DBNull.Value;
                                                                                    cmd2.Parameters["ord_state"].Value = (string)rA["ORD_STATE"];
                                                                                    cmd2.Parameters["ord_date"].Value = rA["ORD_DATE"] ?? DBNull.Value;
                                                                                    cmd2.Parameters["prod_qty"].Value = Convert.ToDouble(rA["PROD_QTY"]);
                                                                                    cmd2.Parameters["qty_supply"].Value = Convert.ToDouble(rA["QTY_SUPPLY"]);
                                                                                    cmd2.Parameters["qty_demand"].Value = Convert.ToDouble(rA["QTY_DEMAND"]);
                                                                                    cmd2.Parameters["dat_creat"].Value = rA["CREAT_DATE"] ?? DBNull.Value;
                                                                                    cmd2.Parameters["chksum"].Value = Convert.ToInt32(rA["CHKSUM"]);
                                                                                    cmd2.Parameters["id"].Value = (Guid)rA["ID"];
                                                                                    cmd2.ExecuteNonQuery();
                                                                                }
                                                                                //         Log("END MODIFY DOP_ord for part1:" + (DateTime.Now - start));
                                                                            }
                                                                            TR_updt.Commit();
                                                                        }
                                                                    }
                                                                    // DODAJ dane
                                                                    rwA = tmp_rows.Select("stat='ADD'");
                                                                    //    Log("RECORDS DOP_ord add1: " + rwA.Length);
                                                                    if (rwA.Length > 0)
                                                                    {
                                                                        using (NpgsqlTransaction TR_insr = conB.BeginTransaction())
                                                                        {
                                                                            using (NpgsqlCommand cmd2 = new NpgsqlCommand("" +
                                                                                "INSERT INTO public.ord_demands" +
                                                                                "(dop, dop_lin, data_dop, day_shift, order_no, line_no, rel_no, int_ord ,contract, order_supp_dmd, wrkc, next_wrkc, part_no, descr, part_code," +
                                                                                " date_required, ord_state, ord_date, prod_qty, qty_supply, qty_demand,dat_creat , chksum, id) " +
                                                                                "VALUES " +
                                                                                "(@dop,@dop_lin,@data_dop,@day_shift,@order_no,@line_no,@rel_no,@int_ord,@contract,@order_supp_dmd,@wrkc,@next_wrkc,@part_no,@descr," +
                                                                                "@part_code,@date_required,@ord_state,@ord_date,@prod_qty,@qty_supply,@qty_demand,@dat_creat,@chksum,@id);", conB))
                                                                            {
                                                                                cmd2.Parameters.Add("dop", NpgsqlTypes.NpgsqlDbType.Integer);
                                                                                cmd2.Parameters.Add("dop_lin", NpgsqlTypes.NpgsqlDbType.Integer);
                                                                                cmd2.Parameters.Add("data_dop", NpgsqlTypes.NpgsqlDbType.Date);
                                                                                cmd2.Parameters.Add("day_shift", NpgsqlTypes.NpgsqlDbType.Integer);
                                                                                cmd2.Parameters.Add("order_no", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                                                cmd2.Parameters.Add("line_no", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                                                cmd2.Parameters.Add("rel_no", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                                                cmd2.Parameters.Add("int_ord", NpgsqlTypes.NpgsqlDbType.Integer);
                                                                                cmd2.Parameters.Add("contract", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                                                cmd2.Parameters.Add("order_supp_dmd", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                                                cmd2.Parameters.Add("wrkc", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                                                cmd2.Parameters.Add("next_wrkc", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                                                cmd2.Parameters.Add("part_no", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                                                cmd2.Parameters.Add("descr", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                                                cmd2.Parameters.Add("part_code", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                                                cmd2.Parameters.Add("date_required", NpgsqlTypes.NpgsqlDbType.Date);
                                                                                cmd2.Parameters.Add("ord_state", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                                                cmd2.Parameters.Add("ord_date", NpgsqlTypes.NpgsqlDbType.Date);
                                                                                cmd2.Parameters.Add("prod_qty", NpgsqlTypes.NpgsqlDbType.Double);
                                                                                cmd2.Parameters.Add("qty_supply", NpgsqlTypes.NpgsqlDbType.Double);
                                                                                cmd2.Parameters.Add("qty_demand", NpgsqlTypes.NpgsqlDbType.Double);
                                                                                cmd2.Parameters.Add("dat_creat", NpgsqlTypes.NpgsqlDbType.Date);
                                                                                cmd2.Parameters.Add("chksum", NpgsqlTypes.NpgsqlDbType.Integer);
                                                                                cmd2.Parameters.Add("id", NpgsqlTypes.NpgsqlDbType.Uuid);
                                                                                cmd2.Prepare();
                                                                                foreach (DataRow rA in rwA)
                                                                                {
                                                                                    cmd2.Parameters["dop"].Value = Convert.ToInt32(rA["DOP"]);
                                                                                    cmd2.Parameters["dop_lin"].Value = Convert.ToInt32(rA["DOP_LIN"]);
                                                                                    cmd2.Parameters["data_dop"].Value = rA["DATA_DOP"] ?? DBNull.Value;
                                                                                    cmd2.Parameters["day_shift"].Value = Convert.ToInt32(rA["DAY_SHIFT"]);
                                                                                    cmd2.Parameters["order_no"].Value = (string)rA["ORDER_NO"];
                                                                                    cmd2.Parameters["line_no"].Value = (string)rA["LINE_NO"];
                                                                                    cmd2.Parameters["rel_no"].Value = (string)rA["REL_NO"];
                                                                                    cmd2.Parameters["int_ord"].Value = Convert.ToInt32(rA["INT_ORD"]);
                                                                                    cmd2.Parameters["contract"].Value = (string)rA["CONTRACT"];
                                                                                    cmd2.Parameters["order_supp_dmd"].Value = (string)rA["ORDER_SUPPLY_DEMAND_TYPE"];
                                                                                    cmd2.Parameters["wrkc"].Value = (string)rA["WRKC"];
                                                                                    cmd2.Parameters["next_wrkc"].Value = (string)rA["NEXT_WRKC"];
                                                                                    cmd2.Parameters["part_no"].Value = (string)rA["PART_NO"];
                                                                                    cmd2.Parameters["descr"].Value = (string)rA["DESCR"];
                                                                                    cmd2.Parameters["part_code"].Value = (string)rA["PART_CODE"];
                                                                                    cmd2.Parameters["date_required"].Value = rA["DATE_REQUIRED"] ?? DBNull.Value;
                                                                                    cmd2.Parameters["ord_state"].Value = (string)rA["ORD_STATE"];
                                                                                    cmd2.Parameters["ord_date"].Value = rA["ORD_DATE"] ?? DBNull.Value;
                                                                                    cmd2.Parameters["prod_qty"].Value = Convert.ToDouble(rA["PROD_QTY"]);
                                                                                    cmd2.Parameters["qty_supply"].Value = Convert.ToDouble(rA["QTY_SUPPLY"]);
                                                                                    cmd2.Parameters["qty_demand"].Value = Convert.ToDouble(rA["QTY_DEMAND"]);
                                                                                    cmd2.Parameters["dat_creat"].Value = rA["CREAT_DATE"] ?? DBNull.Value;
                                                                                    cmd2.Parameters["chksum"].Value = Convert.ToInt32(rA["CHKSUM"]);
                                                                                    cmd2.Parameters["id"].Value = System.Guid.NewGuid();
                                                                                    cmd2.ExecuteNonQuery();
                                                                                }
                                                                                //       Log("END ADD DOP_ord for part1:" + (DateTime.Now - start));
                                                                            }
                                                                            TR_insr.Commit();
                                                                        }
                                                                    }
                                                                    if (era_sourc.Rows.Count > 0)
                                                                    {
                                                                        using (NpgsqlTransaction TR_dlt = conB.BeginTransaction())
                                                                        {
                                                                            using (NpgsqlCommand cmd2 = new NpgsqlCommand("delete from ord_demands where \"id\"=@id", conB))
                                                                            {
                                                                                cmd2.Parameters.Add("@id", NpgsqlTypes.NpgsqlDbType.Uuid);
                                                                                cmd2.Prepare();
                                                                                foreach (DataRowView rA in era_sourc.DefaultView)
                                                                                {
                                                                                    cmd2.Parameters[0].Value = rA["id"];
                                                                                    cmd2.ExecuteNonQuery();
                                                                                }
                                                                                //          Log("ERASE dont match DOP_ord for part1" + (DateTime.Now - start));
                                                                            }
                                                                            TR_dlt.Commit();
                                                                        }
                                                                    }
                                                                }
                                                                catch (Exception e)
                                                                {
                                                                    Log("Błąd modyfikacji tabeli ord_dmd1 postegrsql:" + e);
                                                                }
                                                            }
                                                        }
                                                    }
                                                    //   Log("read get_ord for part1:" + part_no + " Time:" + (DateTime.Now - start));
                                                    if (bgc == 10)
                                                    {
                                                        GC.Collect();
                                                        bgc = 0;
                                                    }
                                                }
                                            }
                                            part_no = (string)row["part_no"];
                                            first = (DateTime)row["Dat"];
                                            last = (DateTime)row["Dat"];
                                        }
                                        last = (DateTime)row["Dat"];
                                        dt_len++;
                                    }
                                }
                            }
                            using (NpgsqlCommand cmd1 = new NpgsqlCommand("" +
                                "UPDATE public.datatbles " +
                                "SET last_modify=current_timestamp, in_progress=false,updt_errors=false " +
                                "" +
                                "WHERE table_name='worker2'", conB))
                            {
                                cmd1.ExecuteNonQuery();
                            }
                        }
                    }
                }
                GC.Collect();
                Log("End WORKER2");
                return 0;
            }
            catch (Exception e)
            {
                Log("Błąd odczytu informacji z ORACLE :" + e);
                return 1;
            }

        }
        private async Task<int> Get_ord_dem2()
        {
            try
            {
                using (OracleConnection conO = new OracleConnection("Password =pass;User ID = user; Data Source = prod8"))
                {
                    await conO.OpenAsync();
                    using (NpgsqlConnection conB = new NpgsqlConnection(npA))
                    {
                        conB.Open();
                        using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                            "UPDATE public.datatbles " +
                            "SET start_update=current_timestamp, in_progress=true " +
                            "WHERE table_name='worker3'", conB))
                        {
                            cmd.ExecuteNonQuery();
                        }
                        Log("start get_ord2:" + (DateTime.Now - start));
                        using (OracleCommand cmd = new OracleCommand("" +
                            "SELECT  a.DOP,Nvl(a.LINE_ITEM_NO,a.DOP_LIN) DOP_LIN,Nvl(ifsapp.dop_order_api.Get_Revised_Due_Date(a.DOP,1),a.DATE_REQUIRED) Data_dop," +
                            "Nvl(ifsapp.work_time_calendar_api.Get_Work_Days_Between(ifsapp.site_api.Get_Manuf_Calendar_Id(a.CONTRACT), ifsapp.dop_order_api.Get_Revised_Start_Date(a.DOP, a.DOP_LIN)," +
                                " ifsapp.dop_order_api.Get_Revised_Due_Date(a.DOP, 1)) + ifsapp.dop_order_api.Dop_Order_Slack(a.DOP, a.DOP_LIN) + 1, 0) DAy_shift," +
                                "a.ORDER_NO, a.LINE_NO, a.REL_NO,cast(Decode(LENGTH(TRIM(TRANSLATE(a.ORDER_NO, ' +-.0123456789',' '))),NULL,a.ORDER_NO,owa_opt_lock.checksum(a.ORDER_NO)*-1) as INT) int_ord," +
                                " a.CONTRACT, a.ORDER_SUPPLY_DEMAND_TYPE, a.WRKC, a.NEXT_WRKC, a.PART_NO," +
                                " ifsapp.inventory_part_api.Get_Description(CONTRACT, a.PART_NO) Descr, a.PART_CODE, a.DATE_REQUIRED,a.ORD_STATE, a.ORD_DATE,cast(a.PROD_QTY as float) PROD_QTY," +
                                "cast(a.QTY_SUPPLY as float) QTY_SUPPLY,cast(a.QTY_DEMAND as float) QTY_DEMAND,To_Date(creat_date) creat_date, chksum " +
                                "from " +
                                    "(SELECT Nvl(ifsapp.dop_supply_shop_ord_api.Get_C_Dop_Id(order_no, line_no, rel_no), 0) DOP," +
                                    "Nvl(REPLACE(SubStr(ifsapp.shop_ord_api.Get_Source(order_no, line_no, rel_no), InStr(ifsapp.shop_ord_api.Get_Source(order_no, line_no, rel_no), '^', 10)), '^', ''), 0) DOP_LIN," +
                                    "order_no, line_no, rel_no, LINE_ITEM_NO, ifsapp.shop_ord_api.Get_Part_No(order_no, line_no, rel_no) Cust_part_no, CONTRACT, ORDER_SUPPLY_DEMAND_TYPE," +
                                    "ifsapp.shop_order_operation_list_api.Get_Next_Op_Work_Center(order_no, line_no, rel_no, ifsapp.shop_order_operation_list_api.Get_Prev_Non_Parallel_Op(order_no, line_no, rel_no, 1, 0)) WRKC," +
                                    "ifsapp.shop_order_operation_list_api.Get_Next_Op_Work_Center(order_no, line_no, rel_no, ifsapp.shop_order_operation_list_api.Get_Prev_Non_Parallel_Op(order_no, line_no, rel_no, 2, 0)) NEXT_WRKC," +
                                    "PART_NO, STATUS_CODE PART_CODE, DATE_REQUIRED, ifsapp.shop_ord_api.Get_State(order_no, line_no, rel_no) ORD_STATE, ifsapp.shop_ord_api.Get_Revised_Due_Date(order_no, line_no, rel_no) ORD_date," +
                                    "Nvl(ifsapp.shop_ord_api.Get_Revised_Qty_Due(order_no, line_no, rel_no)-ifsapp.shop_ord_api.Get_Qty_Complete(order_no, line_no, rel_no), 0) Prod_QTY,0 QTY_SUPPLY, QTY_DEMAND," +
                                    " ifsapp.shop_ord_api.Get_Date_Entered(order_no, line_no, rel_no) creat_date, owa_opt_lock.checksum(ROWID||QTY_DEMAND||QTY_PEGGED||QTY_RESERVED||" +
                                    "To_Char(ifsapp.shop_order_operation_api.Get_Op_Start_Date(order_no,line_no,rel_no," +
                                        "ifsapp.shop_order_operation_list_api.Get_Prev_Non_Parallel_Op (order_no,line_no,rel_no,2,0)),'YYYYMMDDHH24miss')) chksum  " +
                                    "FROM " +
                                    "ifsapp.shop_material_alloc_demand " +
                                    "WHERE part_no = :part_no AND DATE_REQUIRED between :Dat and :Dat1 " +
                                    "UNION ALL " +
                                    "sELECT To_Number(ORDER_NO) DOP, LINE_NO DOP_LIN, '0' ORDER_NO, '*' LINE_NO, '*' REL_NO, LINE_ITEM_NO, NULL Cust_part_no, CONTRACT, ORDER_SUPPLY_DEMAND_TYPE, ' ' WRKC, ' ' NEXT_WRKC," +
                                    " PART_NO, STATUS_CODE PART_CODE,DATE_REQUIRED, ifsapp.dop_head_api.Get_Status(ORDER_NO) ORD_STATE, ifsapp.dop_head_api.Get_Due_Date(ORDER_NO) ORD_DATE," +
                                    " ifsapp.dop_head_api.Get_Qty_Demand(order_no) PROD_QTY, 0 QTY_SUPPLY, QTY_DEMAND, NULL creat_date,owa_opt_lock.checksum(order_no||QTY_DEMAND||DATE_REQUIRED||ORDER_NO||LINE_NO||INFO) chksum  " +
                                    "FROM " +
                                    "ifsapp.dop_order_demand_ext " +
                                    "WHERE part_no = :part_no AND DATE_REQUIRED between :Dat and :Dat1 " +
                                    "UNION ALL " +
                                    "SELECT ifsapp.customer_order_line_api.Get_Pre_Accounting_Id(ORDER_NO, LINE_NO, REL_NO, LINE_ITEM_NO) DOP, '0' DOP_LIN, ORDER_NO, LINE_NO, REL_NO, LINE_ITEM_NO, PART_NO Cust_part_no," +
                                    " CONTRACT, ORDER_SUPPLY_DEMAND_TYPE, ' ' WRKC, ' ' NEXT_WRKC, PART_NO, STATUS_CODE PART_CODE, DATE_REQUIRED,STATUS_CODE ORD_STATE, DATE_REQUIRED ORD_DATE, 0 PROD_QTY, 0 QTY_SUPPLY," +
                                    " QTY_DEMAND, ifsapp.customer_order_line_api.Get_Date_Entered(order_no, line_no, rel_no, line_item_no) creat_date, " +
                                    "owa_opt_lock.checksum(ROW_ID||QTY_DEMAND||DATE_REQUIRED||QTY_PEGGED||QTY_RESERVED) chksum FROM ifsapp.customer_order_line_demand_oe " +
                                    "WHERE part_no = :part_no AND DATE_REQUIRED between :Dat and :Dat1 " +
                                    "UNION ALL " +
                                    "SELECT 0 DOP, '0' DOP_LIN, a.ORDER_NO, a.LINE_NO, a.REL_NO, a.LINE_ITEM_NO, a.PART_NO Cust_part_no, a.CONTRACT, a.ORDER_SUPPLY_DEMAND_TYPE, ' ' WRKC, ' ' NEXT_WRKC, a.PART_NO," +
                                    " a.STATUS_CODE PART_CODE, a.DATE_REQUIRED, a.STATUS_CODE ORD_STATE, a.DATE_REQUIRED ORD_DATE, 0 PROD_QTY, 0 QTY_SUPPLY, a.QTY_DEMAND, b.DATE_ENTERED creat_date, " +
                                    "owa_opt_lock.checksum(a.ROWID||QTY_DEMAND||DATE_REQUIRED||a.STATUS_CODE) chksum  FROM ifsapp.material_requis_line_demand_oe a, ifsapp.material_requis_line b " +
                                    "WHERE b.OBJID = a.ROW_ID and  a.part_no = :part_no AND a.DATE_REQUIRED between :Dat and :Dat1  " +
                                    "UNION ALL  " +
                                    "SELECT 0 DOP, '0' DOP_LIN, ORDER_NO, LINE_NO, REL_NO, LINE_ITEM_NO, PART_NO Cust_part_no, CONTRACT, ORDER_SUPPLY_DEMAND_TYPE, ' ' WRKC, ' ' NEXT_WRKC, PART_NO, STATUS_CODE PART_CODE, " +
                                    "DATE_REQUIRED, STATUS_CODE ORD_STATE, DATE_REQUIRED ORD_DATE, 0 PROD_QTY, QTY_SUPPLY, 0 QTY_DEMAND, " +
                                    "ifsapp.customer_order_line_api.Get_Date_Entered(order_no, line_no, rel_no, line_item_no) creat_date, " +
                                    "owa_opt_lock.checksum(ROWID||QTY_SUPPLY||DATE_REQUIRED) chksum " +
                                    "FROM " +
                                    "ifsapp.purchase_order_line_supply " +
                                    "WHERE part_no = :part_no AND DATE_REQUIRED between :Dat and :Dat1  " +
                                    "UNION ALL " +
                                    "SELECT 0 DOP, '0' DOP_LIN, ORDER_NO, LINE_NO, REL_NO, LINE_ITEM_NO, PART_NO Cust_part_no, CONTRACT, ORDER_SUPPLY_DEMAND_TYPE, ' ' WRKC, ' ' NEXT_WRKC, PART_NO, STATUS_CODE PART_CODE," +
                                    " DATE_REQUIRED, STATUS_CODE ORD_STATE, DATE_REQUIRED ORD_DATE, 0 PROD_QTY, QTY_SUPPLY, 0 QTY_DEMAND, " +
                                    "ifsapp.customer_order_line_api.Get_Date_Entered(order_no, line_no, rel_no, line_item_no) creat_date, " +
                                    "owa_opt_lock.checksum(ROWID||QTY_SUPPLY||DATE_REQUIRED||STATUS_CODE) chksum FROM ifsapp.ARRIVED_PUR_ORDER_EXT " +
                                    "WHERE part_no = :part_no AND DATE_REQUIRED between :Dat and :Dat1 ) a " +
                                "order by to_number(dop) desc,to_number(dop_lin),to_number(int_ord),line_no,rel_no ", conO))
                        {
                            //cmd.FetchSize = cmd.FetchSize * 512;
                            cmd.Parameters.Add(":part_no", OracleDbType.Varchar2);
                            cmd.Parameters.Add(":Dat", OracleDbType.Date);
                            cmd.Parameters.Add(":Dat1", OracleDbType.Date);
                            cmd.Prepare();
                            Log("Prepare command get_ord2:" + (DateTime.Now - start));
                            using (NpgsqlCommand cmd1 = new NpgsqlCommand("select * from public.ord_demands where part_no=@part_no and DATE_REQUIRED between @first and @last order by dop desc,dop_lin,int_ord,LINE_NO,REL_NO;", conB))
                            {
                                cmd1.Parameters.Add("@part_no", NpgsqlTypes.NpgsqlDbType.Varchar);
                                cmd1.Parameters.Add("@first", NpgsqlTypes.NpgsqlDbType.Date);
                                cmd1.Parameters.Add("@last", NpgsqlTypes.NpgsqlDbType.Date);
                                cmd1.Prepare();
                                DataRow[] dt = MShedul.Select("kol = 2");
                                if (dt.Length > 0)
                                {
                                    string part_no = (string)dt[0]["part_no"];
                                    DateTime first = (DateTime)dt[0]["Dat"];
                                    DateTime last = (DateTime)dt[0]["Dat"];
                                    int dt_len = 0;
                                    foreach (DataRow row in dt)
                                    {
                                        if ((string)row["part_no"] != part_no || dt_len == dt.Length - 1)
                                        {
                                            if (first == last) { last = last.AddDays(1); }
                                            cmd.Parameters[0].Value = part_no;
                                            cmd.Parameters[1].Value = first;
                                            cmd.Parameters[2].Value = last;
                                            using (OracleDataReader re = cmd.ExecuteReader())
                                            {
                                                using (DataTable tmp_rows = new DataTable())
                                                {
                                                    tmp_rows.Load(re);
                                                    tmp_rows.Columns.Add("id", System.Type.GetType("System.Guid"));
                                                    tmp_rows.Columns.Add("stat", System.Type.GetType("System.String"));

                                                    //   Log("read get_ord_ORA for part2:" + part_no + " Time:" + (DateTime.Now - start));
                                                    cmd1.Parameters[0].Value = part_no;
                                                    cmd1.Parameters[1].Value = first;
                                                    cmd1.Parameters[2].Value = last;
                                                    using (NpgsqlDataReader po1 = cmd1.ExecuteReader())
                                                    {
                                                        using (DataTable sou_rows = new DataTable())
                                                        {
                                                            DataTable sch = po1.GetSchemaTable();
                                                            foreach (DataRow a in sch.Rows)
                                                            {
                                                                sou_rows.Columns.Add(a["ColumnName"].ToString(), System.Type.GetType(a["DataType"].ToString()));
                                                                sou_rows.Columns[a["ColumnName"].ToString()].AllowDBNull = true;
                                                            }
                                                            sch.Clear();
                                                            sch.Dispose();
                                                            sou_rows.Load(po1);
                                                            //tmp_rows.DefaultView.Sort = "dop desc,dop_lin,int_ord,LINE_NO,REL_NO";
                                                            //      Log("read get_ord_postg for part2:" + part_no + " from:" + first + " To:" + last + " Records ORA:" + tmp_rows.Rows.Count + " Records PSTGR:" + sou_rows.Rows.Count + " Time:" + (DateTime.Now - start)); ;
                                                            int ind_sou_rows = 0;
                                                            int max_sou_rows = sou_rows.Rows.Count;
                                                            using (DataTable era_sourc = sou_rows.Clone())
                                                            {
                                                                foreach (DataColumn a in era_sourc.Columns)
                                                                {
                                                                    a.AllowDBNull = true;
                                                                }
                                                                int counter = -1;
                                                                int max = tmp_rows.Rows.Count;
                                                                foreach (DataRowView rw in tmp_rows.DefaultView)
                                                                {
                                                                    if (counter < max) { counter++; }
                                                                    if (sou_rows.Rows.Count > 0 && max_sou_rows > ind_sou_rows)
                                                                    {
                                                                        while (Convert.ToInt32(rw["dop"]) < Convert.ToInt32(sou_rows.DefaultView[ind_sou_rows].Row["dop"]) || (Convert.ToInt32(rw["dop"]) == Convert.ToInt32(sou_rows.DefaultView[ind_sou_rows].Row["dop"]) && (Convert.ToInt32(rw["dop_lin"]) > Convert.ToInt32(sou_rows.DefaultView[ind_sou_rows].Row["dop_lin"]) || (Convert.ToInt32(rw["dop"]) == Convert.ToInt32(sou_rows.DefaultView[ind_sou_rows].Row["dop"]) && (Convert.ToInt32(rw["dop_lin"]) == Convert.ToInt32(sou_rows.DefaultView[ind_sou_rows].Row["dop_lin"]) && Convert.ToInt32(rw["int_ord"]) > Convert.ToInt32(sou_rows.DefaultView[ind_sou_rows].Row["int_ord"]))))))
                                                                        {
                                                                            // zlecenia wcześniejsze do usunięcia
                                                                            DataRow r = era_sourc.NewRow();
                                                                            for (int i = 0; i < sou_rows.Columns.Count; i++)
                                                                            {
                                                                                r[i] = sou_rows.DefaultView[ind_sou_rows].Row[i];
                                                                            }
                                                                            era_sourc.Rows.Add(r);
                                                                            ind_sou_rows++;
                                                                            if (max_sou_rows <= ind_sou_rows) { break; }
                                                                        }
                                                                        if (max_sou_rows > ind_sou_rows)
                                                                        {
                                                                            if (Convert.ToInt32(rw["dop"]) == Convert.ToInt32(sou_rows.DefaultView[ind_sou_rows].Row["dop"]) && Convert.ToInt32(rw["dop_lin"]) == Convert.ToInt32(sou_rows.DefaultView[ind_sou_rows].Row["dop_lin"]))
                                                                            {
                                                                                if (Convert.ToInt32(rw["int_ord"]) == Convert.ToInt32(sou_rows.DefaultView[ind_sou_rows].Row["int_ord"]) && (string)rw["line_no"] == (string)sou_rows.DefaultView[ind_sou_rows].Row["line_no"] && (string)rw["rel_no"] == (string)sou_rows.DefaultView[ind_sou_rows].Row["rel_no"])
                                                                                {
                                                                                    //rekord istnieje
                                                                                    bool modif = false;
                                                                                    if (Convert.ToInt32(rw["chksum"]) != Convert.ToInt32(sou_rows.DefaultView[ind_sou_rows].Row["chksum"])) { modif = true; }
                                                                                    if (!modif)
                                                                                    {
                                                                                        for (int i = 2; i < tmp_rows.Columns.Count - 2; i++)
                                                                                        {
                                                                                            bool db_or = false;
                                                                                            bool db_pst = false;
                                                                                            if (sou_rows.DefaultView[ind_sou_rows].Row[i] == DBNull.Value || sou_rows.DefaultView[ind_sou_rows].Row[i] == null) { db_pst = true; }
                                                                                            if (rw[i] == DBNull.Value || rw[i] == null) { db_or = true; }
                                                                                            if (i == 3 || i == 7 || (i > 17 && i < 21))
                                                                                            {
                                                                                                if (Convert.ToDouble(rw[i]) != Convert.ToDouble(sou_rows.DefaultView[ind_sou_rows].Row[i])) { modif = true; }
                                                                                            }
                                                                                            else
                                                                                            {
                                                                                                if (!db_or && !db_pst)
                                                                                                {
                                                                                                    if (rw[i].ToString() != sou_rows.DefaultView[ind_sou_rows].Row[i].ToString()) { modif = true; }
                                                                                                }
                                                                                                else
                                                                                                {
                                                                                                    if (db_or != db_pst) { modif = true; }
                                                                                                }
                                                                                            }
                                                                                            if (modif) { break; }
                                                                                        }
                                                                                    }
                                                                                    if (modif)
                                                                                    {
                                                                                        // nastąpiła modyfikacja
                                                                                        rw["stat"] = "MOD";
                                                                                        rw["id"] = sou_rows.DefaultView[ind_sou_rows].Row["id"];
                                                                                    }
                                                                                    ind_sou_rows++;
                                                                                }
                                                                                else
                                                                                {
                                                                                    // brak rekordu dodaję
                                                                                    rw["stat"] = "ADD";
                                                                                    rw["id"] = System.Guid.NewGuid();
                                                                                }
                                                                            }
                                                                            else
                                                                            {
                                                                                // brak rekordu dodaję
                                                                                rw["stat"] = "ADD";
                                                                                rw["id"] = System.Guid.NewGuid();
                                                                            }
                                                                            if (counter >= max - 1 && max_sou_rows > ind_sou_rows)
                                                                            {
                                                                                while (max_sou_rows > ind_sou_rows)
                                                                                {
                                                                                    DataRow r = era_sourc.NewRow();
                                                                                    for (int i = 0; i < sou_rows.Columns.Count; i++)
                                                                                    {
                                                                                        r[i] = sou_rows.DefaultView[ind_sou_rows].Row[i];
                                                                                    }
                                                                                    era_sourc.Rows.Add(r);
                                                                                    ind_sou_rows++;
                                                                                    if (max_sou_rows <= ind_sou_rows) { break; }
                                                                                }
                                                                            }
                                                                        }
                                                                        else
                                                                        {
                                                                            //rekordy do porównania się skończyły
                                                                            rw["stat"] = "ADD";
                                                                        }
                                                                    }
                                                                    else
                                                                    {
                                                                        // nowe dane jeśli brak tabeli PSTGR
                                                                        rw["stat"] = "ADD";
                                                                    }
                                                                }
                                                                //    Log("END compare get_ord for part2:" + part_no + " Time:" + (DateTime.Now - start));
                                                                //Start update
                                                                try
                                                                {
                                                                    DataRow[] rwA = tmp_rows.Select("stat='MOD'");
                                                                    //    Log("RECORDS DOP_ord mod2: " + rwA.Length);
                                                                    if (rwA.Length > 0)
                                                                    {
                                                                        using (NpgsqlTransaction TR_updt = conB.BeginTransaction())
                                                                        {
                                                                            using (NpgsqlCommand cmd2 = new NpgsqlCommand("UPDATE public.ord_demands SET dop=@dop, dop_lin=@dop_lin, data_dop=@data_dop, day_shift=@day_shift, order_no=@order_no, line_no=@line_no, rel_no=@rel_no,int_ord=@int_ord, contract=@contract, order_supp_dmd=@order_supp_dmd, wrkc=@wrkc, next_wrkc=@next_wrkc, part_no=@part_no, descr=@descr, part_code=@part_code, date_required=@date_required, ord_state=@ord_state, ord_date=@ord_date, prod_qty=@prod_qty, qty_supply=@qty_supply, qty_demand=@qty_demand, chksum=@chksum ,dat_creat=@dat_creat where \"id\"=@ID", conB))
                                                                            {
                                                                                cmd2.Parameters.Add("dop", NpgsqlTypes.NpgsqlDbType.Integer);
                                                                                cmd2.Parameters.Add("dop_lin", NpgsqlTypes.NpgsqlDbType.Integer);
                                                                                cmd2.Parameters.Add("data_dop", NpgsqlTypes.NpgsqlDbType.Date);
                                                                                cmd2.Parameters.Add("day_shift", NpgsqlTypes.NpgsqlDbType.Integer);
                                                                                cmd2.Parameters.Add("order_no", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                                                cmd2.Parameters.Add("line_no", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                                                cmd2.Parameters.Add("rel_no", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                                                cmd2.Parameters.Add("int_ord", NpgsqlTypes.NpgsqlDbType.Integer);
                                                                                cmd2.Parameters.Add("contract", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                                                cmd2.Parameters.Add("order_supp_dmd", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                                                cmd2.Parameters.Add("wrkc", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                                                cmd2.Parameters.Add("next_wrkc", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                                                cmd2.Parameters.Add("part_no", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                                                cmd2.Parameters.Add("descr", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                                                cmd2.Parameters.Add("part_code", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                                                cmd2.Parameters.Add("date_required", NpgsqlTypes.NpgsqlDbType.Date);
                                                                                cmd2.Parameters.Add("ord_state", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                                                cmd2.Parameters.Add("ord_date", NpgsqlTypes.NpgsqlDbType.Date);
                                                                                cmd2.Parameters.Add("prod_qty", NpgsqlTypes.NpgsqlDbType.Double);
                                                                                cmd2.Parameters.Add("qty_supply", NpgsqlTypes.NpgsqlDbType.Double);
                                                                                cmd2.Parameters.Add("qty_demand", NpgsqlTypes.NpgsqlDbType.Double);
                                                                                cmd2.Parameters.Add("dat_creat", NpgsqlTypes.NpgsqlDbType.Date);
                                                                                cmd2.Parameters.Add("chksum", NpgsqlTypes.NpgsqlDbType.Integer);
                                                                                cmd2.Parameters.Add("id", NpgsqlTypes.NpgsqlDbType.Uuid);
                                                                                cmd2.Prepare();
                                                                                foreach (DataRow rA in rwA)
                                                                                {
                                                                                    cmd2.Parameters["dop"].Value = Convert.ToInt32(rA["DOP"]);
                                                                                    cmd2.Parameters["dop_lin"].Value = Convert.ToInt32(rA["DOP_LIN"]);
                                                                                    cmd2.Parameters["data_dop"].Value = rA["DATA_DOP"] ?? DBNull.Value;
                                                                                    cmd2.Parameters["day_shift"].Value = Convert.ToInt32(rA["DAY_SHIFT"]);
                                                                                    cmd2.Parameters["order_no"].Value = (string)rA["ORDER_NO"];
                                                                                    cmd2.Parameters["line_no"].Value = (string)rA["LINE_NO"];
                                                                                    cmd2.Parameters["rel_no"].Value = (string)rA["REL_NO"];
                                                                                    cmd2.Parameters["int_ord"].Value = Convert.ToInt32(rA["INT_ORD"]);
                                                                                    cmd2.Parameters["contract"].Value = (string)rA["CONTRACT"];
                                                                                    cmd2.Parameters["order_supp_dmd"].Value = (string)rA["ORDER_SUPPLY_DEMAND_TYPE"];
                                                                                    cmd2.Parameters["wrkc"].Value = (string)rA["WRKC"];
                                                                                    cmd2.Parameters["next_wrkc"].Value = (string)rA["NEXT_WRKC"];
                                                                                    cmd2.Parameters["part_no"].Value = (string)rA["PART_NO"];
                                                                                    cmd2.Parameters["descr"].Value = (string)rA["DESCR"];
                                                                                    cmd2.Parameters["part_code"].Value = (string)rA["PART_CODE"];
                                                                                    cmd2.Parameters["date_required"].Value = rA["DATE_REQUIRED"] ?? DBNull.Value;
                                                                                    cmd2.Parameters["ord_state"].Value = (string)rA["ORD_STATE"];
                                                                                    cmd2.Parameters["ord_date"].Value = rA["ORD_DATE"] ?? DBNull.Value; ;
                                                                                    cmd2.Parameters["prod_qty"].Value = Convert.ToDouble(rA["PROD_QTY"]);
                                                                                    cmd2.Parameters["qty_supply"].Value = Convert.ToDouble(rA["QTY_SUPPLY"]);
                                                                                    cmd2.Parameters["qty_demand"].Value = Convert.ToDouble(rA["QTY_DEMAND"]);
                                                                                    cmd2.Parameters["dat_creat"].Value = rA["CREAT_DATE"] ?? DBNull.Value;
                                                                                    cmd2.Parameters["chksum"].Value = Convert.ToInt32(rA["CHKSUM"]);
                                                                                    cmd2.Parameters["id"].Value = (Guid)rA["ID"];
                                                                                    cmd2.ExecuteNonQuery();
                                                                                }
                                                                                //        Log("END MODIFY DOP_ord for part2:" + (DateTime.Now - start));
                                                                            }
                                                                            TR_updt.Commit();
                                                                        }
                                                                    }
                                                                    // DODAJ dane
                                                                    rwA = tmp_rows.Select("stat='ADD'");
                                                                    //        Log("RECORDS DOP_ord add2: " + rwA.Length);
                                                                    if (rwA.Length > 0)
                                                                    {

                                                                        using (NpgsqlTransaction TR_insert = conB.BeginTransaction())
                                                                        {
                                                                            using (NpgsqlCommand cmd2 = new NpgsqlCommand("INSERT INTO public.ord_demands(dop, dop_lin, data_dop, day_shift, order_no, line_no, rel_no, int_ord ,contract, order_supp_dmd, wrkc, next_wrkc, part_no, descr, part_code, date_required, ord_state, ord_date, prod_qty, qty_supply, qty_demand,dat_creat , chksum, id) VALUES (@dop,@dop_lin,@data_dop,@day_shift,@order_no,@line_no,@rel_no,@int_ord,@contract,@order_supp_dmd,@wrkc,@next_wrkc,@part_no,@descr,@part_code,@date_required,@ord_state,@ord_date,@prod_qty,@qty_supply,@qty_demand,@dat_creat,@chksum,@id);", conB))
                                                                            {
                                                                                cmd2.Parameters.Add("dop", NpgsqlTypes.NpgsqlDbType.Integer);
                                                                                cmd2.Parameters.Add("dop_lin", NpgsqlTypes.NpgsqlDbType.Integer);
                                                                                cmd2.Parameters.Add("data_dop", NpgsqlTypes.NpgsqlDbType.Date);
                                                                                cmd2.Parameters.Add("day_shift", NpgsqlTypes.NpgsqlDbType.Integer);
                                                                                cmd2.Parameters.Add("order_no", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                                                cmd2.Parameters.Add("line_no", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                                                cmd2.Parameters.Add("rel_no", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                                                cmd2.Parameters.Add("int_ord", NpgsqlTypes.NpgsqlDbType.Integer);
                                                                                cmd2.Parameters.Add("contract", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                                                cmd2.Parameters.Add("order_supp_dmd", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                                                cmd2.Parameters.Add("wrkc", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                                                cmd2.Parameters.Add("next_wrkc", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                                                cmd2.Parameters.Add("part_no", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                                                cmd2.Parameters.Add("descr", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                                                cmd2.Parameters.Add("part_code", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                                                cmd2.Parameters.Add("date_required", NpgsqlTypes.NpgsqlDbType.Date);
                                                                                cmd2.Parameters.Add("ord_state", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                                                cmd2.Parameters.Add("ord_date", NpgsqlTypes.NpgsqlDbType.Date);
                                                                                cmd2.Parameters.Add("prod_qty", NpgsqlTypes.NpgsqlDbType.Double);
                                                                                cmd2.Parameters.Add("qty_supply", NpgsqlTypes.NpgsqlDbType.Double);
                                                                                cmd2.Parameters.Add("qty_demand", NpgsqlTypes.NpgsqlDbType.Double);
                                                                                cmd2.Parameters.Add("dat_creat", NpgsqlTypes.NpgsqlDbType.Date);
                                                                                cmd2.Parameters.Add("chksum", NpgsqlTypes.NpgsqlDbType.Integer);
                                                                                cmd2.Parameters.Add("id", NpgsqlTypes.NpgsqlDbType.Uuid);
                                                                                cmd2.Prepare();
                                                                                foreach (DataRow rA in rwA)
                                                                                {
                                                                                    cmd2.Parameters["dop"].Value = Convert.ToInt32(rA["DOP"]);
                                                                                    cmd2.Parameters["dop_lin"].Value = Convert.ToInt32(rA["DOP_LIN"]);
                                                                                    cmd2.Parameters["data_dop"].Value = rA["DATA_DOP"] ?? DBNull.Value; ;
                                                                                    cmd2.Parameters["day_shift"].Value = Convert.ToInt32(rA["DAY_SHIFT"]);
                                                                                    cmd2.Parameters["order_no"].Value = (string)rA["ORDER_NO"];
                                                                                    cmd2.Parameters["line_no"].Value = (string)rA["LINE_NO"];
                                                                                    cmd2.Parameters["rel_no"].Value = (string)rA["REL_NO"];
                                                                                    cmd2.Parameters["int_ord"].Value = Convert.ToInt32(rA["INT_ORD"]);
                                                                                    cmd2.Parameters["contract"].Value = (string)rA["CONTRACT"];
                                                                                    cmd2.Parameters["order_supp_dmd"].Value = (string)rA["ORDER_SUPPLY_DEMAND_TYPE"];
                                                                                    cmd2.Parameters["wrkc"].Value = (string)rA["WRKC"];
                                                                                    cmd2.Parameters["next_wrkc"].Value = (string)rA["NEXT_WRKC"];
                                                                                    cmd2.Parameters["part_no"].Value = (string)rA["PART_NO"];
                                                                                    cmd2.Parameters["descr"].Value = (string)rA["DESCR"];
                                                                                    cmd2.Parameters["part_code"].Value = (string)rA["PART_CODE"];
                                                                                    cmd2.Parameters["date_required"].Value = rA["DATE_REQUIRED"] ?? DBNull.Value; ;
                                                                                    cmd2.Parameters["ord_state"].Value = (string)rA["ORD_STATE"];
                                                                                    cmd2.Parameters["ord_date"].Value = rA["ORD_DATE"] ?? DBNull.Value; ;
                                                                                    cmd2.Parameters["prod_qty"].Value = Convert.ToDouble(rA["PROD_QTY"]);
                                                                                    cmd2.Parameters["qty_supply"].Value = Convert.ToDouble(rA["QTY_SUPPLY"]);
                                                                                    cmd2.Parameters["qty_demand"].Value = Convert.ToDouble(rA["QTY_DEMAND"]);
                                                                                    cmd2.Parameters["dat_creat"].Value = rA["CREAT_DATE"] ?? DBNull.Value; ;
                                                                                    cmd2.Parameters["chksum"].Value = Convert.ToInt32(rA["CHKSUM"]);
                                                                                    cmd2.Parameters["id"].Value = System.Guid.NewGuid();
                                                                                    cmd2.ExecuteNonQuery();
                                                                                }
                                                                                //          Log("END ADD DOP_ord for part2:" + (DateTime.Now - start));
                                                                            }
                                                                            TR_insert.Commit();
                                                                        }
                                                                    }
                                                                    if (era_sourc.Rows.Count > 0)
                                                                    {

                                                                        using (NpgsqlTransaction TR_delet = conB.BeginTransaction())
                                                                        {
                                                                            using (NpgsqlCommand cmd2 = new NpgsqlCommand("delete from ord_demands where \"id\"=@id", conB))
                                                                            {
                                                                                cmd2.Parameters.Add("@id", NpgsqlTypes.NpgsqlDbType.Uuid);
                                                                                cmd2.Prepare();
                                                                                foreach (DataRowView rA in era_sourc.DefaultView)
                                                                                {
                                                                                    cmd2.Parameters[0].Value = rA["id"];
                                                                                    cmd2.ExecuteNonQuery();
                                                                                }
                                                                                //             Log("ERASE dont match DOP_ord for part2" + (DateTime.Now - start));
                                                                            }
                                                                            TR_delet.Commit();
                                                                        }
                                                                    }
                                                                }
                                                                catch (Exception e)
                                                                {
                                                                    Log("Błąd modyfikacji tabeli ord_dmd2 postegrsql:" + e);
                                                                }
                                                            }
                                                        }
                                                    }
                                                    //      Log("read get_ord for part2:" + part_no + " Time:" + (DateTime.Now - start));
                                                }
                                            }
                                            part_no = (string)row["part_no"];
                                            first = (DateTime)row["Dat"];
                                            last = (DateTime)row["Dat"];
                                        }
                                        last = (DateTime)row["Dat"];
                                        dt_len++;
                                    }
                                }

                            }
                            using (NpgsqlCommand cmd1 = new NpgsqlCommand("UPDATE public.datatbles SET last_modify=current_timestamp, in_progress=false,updt_errors=false WHERE table_name='worker3'", conB))
                            {
                                cmd1.ExecuteNonQuery();
                            }
                        }
                    }
                }
                GC.Collect();
                Log("End WORKER3");
                return 0;
            }
            catch (Exception e)
            {
                Log("Błąd odczytu informacji z ORACLE :" + e);
                return 1;
            }

        }
        private async Task<int> Get_ord_dem3()
        {
            try
            {
                using (OracleConnection conO = new OracleConnection("Password = pass;User ID = user; Data Source = prod8"))
                {
                    await conO.OpenAsync();
                    using (NpgsqlConnection conB = new NpgsqlConnection(npA))
                    {
                        conB.Open();
                        using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                            "UPDATE public.datatbles " +
                            "SET start_update=current_timestamp, in_progress=true " +
                            "WHERE table_name='worker4'", conB))
                        {
                            cmd.ExecuteNonQuery();
                        }
                        Log("start get_ord3:" + (DateTime.Now - start));
                        using (OracleCommand cmd = new OracleCommand("" +
                            "SELECT a.DOP,Nvl(a.LINE_ITEM_NO,a.DOP_LIN) DOP_LIN,Nvl(ifsapp.dop_order_api.Get_Revised_Due_Date(a.DOP,1),a.DATE_REQUIRED) Data_dop," +
                            "Nvl(ifsapp.work_time_calendar_api.Get_Work_Days_Between(ifsapp.site_api.Get_Manuf_Calendar_Id(a.CONTRACT), ifsapp.dop_order_api.Get_Revised_Start_Date(a.DOP, a.DOP_LIN), " +
                            "ifsapp.dop_order_api.Get_Revised_Due_Date(a.DOP, 1)) + ifsapp.dop_order_api.Dop_Order_Slack(a.DOP, a.DOP_LIN) + 1, 0) DAy_shift,a.ORDER_NO, a.LINE_NO, a.REL_NO," +
                            "cast(Decode(LENGTH(TRIM(TRANSLATE(a.ORDER_NO, ' +-.0123456789',' '))),NULL,a.ORDER_NO,owa_opt_lock.checksum(a.ORDER_NO)*-1) as INT) int_ord, a.CONTRACT, a.ORDER_SUPPLY_DEMAND_TYPE, a.WRKC, " +
                            "a.NEXT_WRKC, a.PART_NO, ifsapp.inventory_part_api.Get_Description(CONTRACT, a.PART_NO) Descr, a.PART_CODE, a.DATE_REQUIRED,a.ORD_STATE, a.ORD_DATE,cast(a.PROD_QTY as float) PROD_QTY," +
                            "cast(a.QTY_SUPPLY as float) QTY_SUPPLY, cast(a.QTY_DEMAND as float) QTY_DEMAND,To_Date(creat_date) creat_date, chksum " +
                            "from " +
                                "(SELECT Nvl(ifsapp.dop_supply_shop_ord_api.Get_C_Dop_Id(order_no, line_no, rel_no), 0) DOP," +
                                "Nvl(REPLACE(SubStr(ifsapp.shop_ord_api.Get_Source(order_no, line_no, rel_no), InStr(ifsapp.shop_ord_api.Get_Source(order_no, line_no, rel_no), '^', 10)), '^', ''), 0) DOP_LIN," +
                                "order_no, line_no, rel_no, LINE_ITEM_NO, ifsapp.shop_ord_api.Get_Part_No(order_no, line_no, rel_no) Cust_part_no, CONTRACT, ORDER_SUPPLY_DEMAND_TYPE," +
                                "ifsapp.shop_order_operation_list_api.Get_Next_Op_Work_Center(order_no, line_no, rel_no, ifsapp.shop_order_operation_list_api.Get_Prev_Non_Parallel_Op(order_no, line_no, rel_no, 1, 0)) WRKC," +
                                "ifsapp.shop_order_operation_list_api.Get_Next_Op_Work_Center(order_no, line_no, rel_no, ifsapp.shop_order_operation_list_api.Get_Prev_Non_Parallel_Op(order_no, line_no, rel_no, 2, 0)) NEXT_WRKC," +
                                "PART_NO, STATUS_CODE PART_CODE, DATE_REQUIRED, ifsapp.shop_ord_api.Get_State(order_no, line_no, rel_no) ORD_STATE, ifsapp.shop_ord_api.Get_Revised_Due_Date(order_no, line_no, rel_no) ORD_date," +
                                "Nvl(ifsapp.shop_ord_api.Get_Revised_Qty_Due(order_no, line_no, rel_no)-ifsapp.shop_ord_api.Get_Qty_Complete(order_no, line_no, rel_no), 0) Prod_QTY,0 QTY_SUPPLY, QTY_DEMAND, " +
                                "ifsapp.shop_ord_api.Get_Date_Entered(order_no, line_no, rel_no) creat_date,owa_opt_lock.checksum(ROWID||QTY_DEMAND||QTY_PEGGED||QTY_RESERVED||" +
                                    "To_Char(ifsapp.shop_order_operation_api.Get_Op_Start_Date(order_no,line_no,rel_no,ifsapp.shop_order_operation_list_api.Get_Prev_Non_Parallel_Op (order_no,line_no,rel_no,2,0)),'YYYYMMDDHH24miss')) chksum  " +
                                "FROM " +
                                "ifsapp.shop_material_alloc_demand " +
                                "WHERE part_no = :part_no AND DATE_REQUIRED between :Dat and :Dat1 " +
                                "UNION ALL " +
                                "sELECT To_Number(ORDER_NO) DOP, LINE_NO DOP_LIN, '0' ORDER_NO, '*' LINE_NO, '*' REL_NO, LINE_ITEM_NO, NULL Cust_part_no, CONTRACT, ORDER_SUPPLY_DEMAND_TYPE, ' ' WRKC, ' ' NEXT_WRKC, " +
                                "PART_NO, STATUS_CODE PART_CODE,DATE_REQUIRED, ifsapp.dop_head_api.Get_Status(ORDER_NO) ORD_STATE, ifsapp.dop_head_api.Get_Due_Date(ORDER_NO) ORD_DATE, " +
                                "ifsapp.dop_head_api.Get_Qty_Demand(order_no) PROD_QTY, 0 QTY_SUPPLY, QTY_DEMAND, NULL creat_date,owa_opt_lock.checksum(order_no||QTY_DEMAND||DATE_REQUIRED||ORDER_NO||LINE_NO||INFO) chksum  " +
                                "FROM ifsapp.dop_order_demand_ext " +
                                "WHERE part_no = :part_no AND DATE_REQUIRED between :Dat and :Dat1 " +
                                "UNION ALL " +
                                "SELECT ifsapp.customer_order_line_api.Get_Pre_Accounting_Id(ORDER_NO, LINE_NO, REL_NO, LINE_ITEM_NO) DOP, '0' DOP_LIN, ORDER_NO, LINE_NO, REL_NO, LINE_ITEM_NO, PART_NO Cust_part_no, " +
                                "CONTRACT, ORDER_SUPPLY_DEMAND_TYPE, ' ' WRKC, ' ' NEXT_WRKC, PART_NO, STATUS_CODE PART_CODE, DATE_REQUIRED,STATUS_CODE ORD_STATE, DATE_REQUIRED ORD_DATE, 0 PROD_QTY, 0 QTY_SUPPLY, " +
                                "QTY_DEMAND, ifsapp.customer_order_line_api.Get_Date_Entered(order_no, line_no, rel_no, line_item_no) creat_date, " +
                                "owa_opt_lock.checksum(ROW_ID||QTY_DEMAND||DATE_REQUIRED||QTY_PEGGED||QTY_RESERVED) chksum " +
                                "FROM " +
                                "ifsapp.customer_order_line_demand_oe " +
                                "WHERE part_no = :part_no AND DATE_REQUIRED between :Dat and :Dat1 " +
                                "UNION ALL " +
                                "SELECT 0 DOP, '0' DOP_LIN, a.ORDER_NO, a.LINE_NO, a.REL_NO, a.LINE_ITEM_NO, a.PART_NO Cust_part_no, a.CONTRACT, a.ORDER_SUPPLY_DEMAND_TYPE, ' ' WRKC, ' ' NEXT_WRKC, a.PART_NO, " +
                                "a.STATUS_CODE PART_CODE, a.DATE_REQUIRED, a.STATUS_CODE ORD_STATE, a.DATE_REQUIRED ORD_DATE, 0 PROD_QTY, 0 QTY_SUPPLY, a.QTY_DEMAND, b.DATE_ENTERED creat_date, " +
                                "owa_opt_lock.checksum(a.ROWID||QTY_DEMAND||DATE_REQUIRED||a.STATUS_CODE) chksum  " +
                                "FROM " +
                                "ifsapp.material_requis_line_demand_oe a, " +
                                "ifsapp.material_requis_line b " +
                                "WHERE b.OBJID = a.ROW_ID and  a.part_no = :part_no AND a.DATE_REQUIRED between :Dat and :Dat1  " +
                                "UNION ALL  " +
                                "SELECT 0 DOP, '0' DOP_LIN, ORDER_NO, LINE_NO, REL_NO, LINE_ITEM_NO, PART_NO Cust_part_no, CONTRACT, ORDER_SUPPLY_DEMAND_TYPE, ' ' WRKC, ' ' NEXT_WRKC, PART_NO, STATUS_CODE PART_CODE, " +
                                "DATE_REQUIRED, STATUS_CODE ORD_STATE, DATE_REQUIRED ORD_DATE, 0 PROD_QTY, QTY_SUPPLY, 0 QTY_DEMAND," +
                                " ifsapp.customer_order_line_api.Get_Date_Entered(order_no, line_no, rel_no, line_item_no) creat_date, " +
                                "owa_opt_lock.checksum(ROWID||QTY_SUPPLY||DATE_REQUIRED) chksum " +
                                "FROM ifsapp.purchase_order_line_supply " +
                                "WHERE part_no = :part_no AND DATE_REQUIRED between :Dat and :Dat1  " +
                                "UNION ALL " +
                                "SELECT 0 DOP, '0' DOP_LIN, ORDER_NO, LINE_NO, REL_NO, LINE_ITEM_NO, PART_NO Cust_part_no, CONTRACT, ORDER_SUPPLY_DEMAND_TYPE, ' ' WRKC, ' ' NEXT_WRKC, PART_NO, STATUS_CODE PART_CODE, " +
                                "DATE_REQUIRED, STATUS_CODE ORD_STATE, DATE_REQUIRED ORD_DATE, 0 PROD_QTY, QTY_SUPPLY, 0 QTY_DEMAND, " +
                                "ifsapp.customer_order_line_api.Get_Date_Entered(order_no, line_no, rel_no, line_item_no) creat_date, " +
                                "owa_opt_lock.checksum(ROWID||QTY_SUPPLY||DATE_REQUIRED||STATUS_CODE) chksum " +
                                "FROM " +
                                "ifsapp.ARRIVED_PUR_ORDER_EXT " +
                                "WHERE part_no = :part_no AND DATE_REQUIRED between :Dat and :Dat1 ) a " +
                            "order by to_number(dop) desc,to_number(dop_lin),to_number(int_ord),line_no,rel_no ", conO))
                        {
                            //cmd.FetchSize = cmd.FetchSize * 512;
                            cmd.Parameters.Add(":part_no", OracleDbType.Varchar2);
                            cmd.Parameters.Add(":Dat", OracleDbType.Date);
                            cmd.Parameters.Add(":Dat1", OracleDbType.Date);
                            cmd.Prepare();
                            Log("Prepare command get_ord3:" + (DateTime.Now - start));
                            using (NpgsqlCommand cmd1 = new NpgsqlCommand("" +
                                "select * from public.ord_demands " +
                                "where part_no=@part_no and DATE_REQUIRED between @first and @last " +
                                "order by dop desc,dop_lin,int_ord,LINE_NO,REL_NO;", conB))
                            {
                                cmd1.Parameters.Add("@part_no", NpgsqlTypes.NpgsqlDbType.Varchar);
                                cmd1.Parameters.Add("@first", NpgsqlTypes.NpgsqlDbType.Date);
                                cmd1.Parameters.Add("@last", NpgsqlTypes.NpgsqlDbType.Date);
                                cmd1.Prepare();
                                DataRow[] dt = MShedul.Select("kol = 3");
                                if (dt.Length > 0)
                                {
                                    string part_no = (string)dt[0]["part_no"];
                                    DateTime first = (DateTime)dt[0]["Dat"];
                                    DateTime last = (DateTime)dt[0]["Dat"];
                                    int dt_len = 0;
                                    foreach (DataRow row in dt)
                                    {
                                        if ((string)row["part_no"] != part_no || dt_len == dt.Length - 1)
                                        {
                                            if (first == last) { last = last.AddDays(1); }
                                            cmd.Parameters[0].Value = part_no;
                                            cmd.Parameters[1].Value = first;
                                            cmd.Parameters[2].Value = last;
                                            using (OracleDataReader re = cmd.ExecuteReader())
                                            {
                                                using (DataTable tmp_rows = new DataTable())
                                                {
                                                    tmp_rows.Load(re);
                                                    tmp_rows.Columns.Add("id", System.Type.GetType("System.Guid"));
                                                    tmp_rows.Columns.Add("stat", System.Type.GetType("System.String"));

                                                    //      Log("read get_ord_ORA for part3:" + part_no + " from:" + first + " To:" + last + " Time:" + (DateTime.Now - start));
                                                    cmd1.Parameters[0].Value = part_no;
                                                    cmd1.Parameters[1].Value = first;
                                                    cmd1.Parameters[2].Value = last;
                                                    using (NpgsqlDataReader po1 = cmd1.ExecuteReader())
                                                    {
                                                        using (DataTable sou_rows = new DataTable())
                                                        {
                                                            DataTable sch = po1.GetSchemaTable();
                                                            foreach (DataRow a in sch.Rows)
                                                            {
                                                                sou_rows.Columns.Add(a["ColumnName"].ToString(), System.Type.GetType(a["DataType"].ToString()));
                                                                sou_rows.Columns[a["ColumnName"].ToString()].AllowDBNull = true;
                                                            }
                                                            sch.Clear();
                                                            sch.Dispose();
                                                            sou_rows.Load(po1);
                                                            //tmp_rows.DefaultView.Sort = "dop desc,dop_lin,int_ord,LINE_NO,REL_NO";
                                                            //              Log("read get_ord_postg for part3:" + part_no + " from:" + first + " To:" + last + " Records ORA:" + tmp_rows.Rows.Count + " Records PSTGR:" + sou_rows.Rows.Count + " Time:" + (DateTime.Now - start)); ;
                                                            int ind_sou_rows = 0;
                                                            int max_sou_rows = sou_rows.Rows.Count;
                                                            using (DataTable era_sourc = sou_rows.Clone())
                                                            {
                                                                foreach (DataColumn a in era_sourc.Columns)
                                                                {
                                                                    a.AllowDBNull = true;
                                                                }
                                                                int counter = -1;
                                                                int max = tmp_rows.Rows.Count;
                                                                foreach (DataRowView rw in tmp_rows.DefaultView)
                                                                {
                                                                    if (counter < max) { counter++; }
                                                                    if (sou_rows.Rows.Count > 0 && max_sou_rows > ind_sou_rows)
                                                                    {
                                                                        while (Convert.ToInt32(rw["dop"]) < Convert.ToInt32(sou_rows.DefaultView[ind_sou_rows].Row["dop"]) || (Convert.ToInt32(rw["dop"]) == Convert.ToInt32(sou_rows.DefaultView[ind_sou_rows].Row["dop"]) && (Convert.ToInt32(rw["dop_lin"]) > Convert.ToInt32(sou_rows.DefaultView[ind_sou_rows].Row["dop_lin"]) || Convert.ToInt32(rw["int_ord"]) > Convert.ToInt32(sou_rows.DefaultView[ind_sou_rows].Row["int_ord"]))))
                                                                        {
                                                                            // zlecenia wcześniejsze do usunięcia
                                                                            DataRow r = era_sourc.NewRow();
                                                                            for (int i = 0; i < sou_rows.Columns.Count; i++)
                                                                            {
                                                                                r[i] = sou_rows.DefaultView[ind_sou_rows].Row[i];
                                                                            }
                                                                            era_sourc.Rows.Add(r);
                                                                            ind_sou_rows++;
                                                                            if (max_sou_rows <= ind_sou_rows) { break; }
                                                                        }
                                                                        if (max_sou_rows > ind_sou_rows)
                                                                        {
                                                                            if (Convert.ToInt32(rw["dop"]) == Convert.ToInt32(sou_rows.DefaultView[ind_sou_rows].Row["dop"]) && Convert.ToInt32(rw["dop_lin"]) == Convert.ToInt32(sou_rows.DefaultView[ind_sou_rows].Row["dop_lin"]))
                                                                            {
                                                                                if (Convert.ToInt32(rw["int_ord"]) == Convert.ToInt32(sou_rows.DefaultView[ind_sou_rows].Row["int_ord"]) && (string)rw["line_no"] == (string)sou_rows.DefaultView[ind_sou_rows].Row["line_no"] && (string)rw["rel_no"] == (string)sou_rows.DefaultView[ind_sou_rows].Row["rel_no"])
                                                                                {
                                                                                    //rekord istnieje
                                                                                    bool modif = false;
                                                                                    if (Convert.ToInt32(rw["chksum"]) != Convert.ToInt32(sou_rows.DefaultView[ind_sou_rows].Row["chksum"])) { modif = true; }
                                                                                    if (!modif)
                                                                                    {
                                                                                        for (int i = 2; i < tmp_rows.Columns.Count - 2; i++)
                                                                                        {
                                                                                            bool db_or = false;
                                                                                            bool db_pst = false;
                                                                                            if (sou_rows.DefaultView[ind_sou_rows].Row[i] == DBNull.Value || sou_rows.DefaultView[ind_sou_rows].Row[i] == null) { db_pst = true; }
                                                                                            if (rw[i] == DBNull.Value || rw[i] == null) { db_or = true; }
                                                                                            if (i == 3 || i == 7 || (i > 17 && i < 21))
                                                                                            {
                                                                                                if (Convert.ToDouble(rw[i]) != Convert.ToDouble(sou_rows.DefaultView[ind_sou_rows].Row[i])) { modif = true; }
                                                                                            }
                                                                                            else
                                                                                            {
                                                                                                if (!db_or && !db_pst)
                                                                                                {
                                                                                                    if (rw[i].ToString() != sou_rows.DefaultView[ind_sou_rows].Row[i].ToString()) { modif = true; }
                                                                                                }
                                                                                                else
                                                                                                {
                                                                                                    if (db_or != db_pst) { modif = true; }
                                                                                                }
                                                                                            }
                                                                                            if (modif) { break; }
                                                                                        }
                                                                                    }
                                                                                    if (modif)
                                                                                    {
                                                                                        // nastąpiła modyfikacja
                                                                                        rw["stat"] = "MOD";
                                                                                        rw["id"] = sou_rows.DefaultView[ind_sou_rows].Row["id"];
                                                                                    }
                                                                                    ind_sou_rows++;
                                                                                }
                                                                                else
                                                                                {
                                                                                    // brak rekordu dodaję
                                                                                    rw["stat"] = "ADD";
                                                                                    rw["id"] = System.Guid.NewGuid();
                                                                                }
                                                                            }
                                                                            else
                                                                            {
                                                                                // brak rekordu dodaję
                                                                                rw["stat"] = "ADD";
                                                                                rw["id"] = System.Guid.NewGuid();
                                                                            }
                                                                            if (counter >= max - 1 && max_sou_rows > ind_sou_rows)
                                                                            {
                                                                                while (max_sou_rows > ind_sou_rows)
                                                                                {
                                                                                    DataRow r = era_sourc.NewRow();
                                                                                    for (int i = 0; i < sou_rows.Columns.Count; i++)
                                                                                    {
                                                                                        r[i] = sou_rows.DefaultView[ind_sou_rows].Row[i];
                                                                                    }
                                                                                    era_sourc.Rows.Add(r);
                                                                                    ind_sou_rows++;
                                                                                    if (max_sou_rows <= ind_sou_rows) { break; }
                                                                                }
                                                                            }
                                                                        }
                                                                        else
                                                                        {
                                                                            //rekordy do porównania się skończyły
                                                                            rw["stat"] = "ADD";
                                                                        }
                                                                    }
                                                                    else
                                                                    {
                                                                        // nowe dane jeśli brak tabeli PSTGR
                                                                        rw["stat"] = "ADD";
                                                                    }
                                                                }
                                                                //          Log("END compare get_ord for part3:" + part_no + " Time:" + (DateTime.Now - start));
                                                                //Start update
                                                                try
                                                                {
                                                                    DataRow[] rwA = tmp_rows.Select("stat='MOD'");
                                                                    //         Log("RECORDS DOP_ord mod3: " + rwA.Length);
                                                                    if (rwA.Length > 0)
                                                                    {
                                                                        using (NpgsqlTransaction TR_updt = conB.BeginTransaction())
                                                                        {
                                                                            using (NpgsqlCommand cmd2 = new NpgsqlCommand("" +
                                                                                "UPDATE public.ord_demands " +
                                                                                "SET dop=@dop, dop_lin=@dop_lin, data_dop=@data_dop, day_shift=@day_shift, order_no=@order_no, line_no=@line_no, rel_no=@rel_no,int_ord=@int_ord, " +
                                                                                "contract=@contract, order_supp_dmd=@order_supp_dmd, wrkc=@wrkc, next_wrkc=@next_wrkc, part_no=@part_no, descr=@descr, part_code=@part_code, " +
                                                                                "date_required=@date_required, ord_state=@ord_state, ord_date=@ord_date, prod_qty=@prod_qty, qty_supply=@qty_supply, qty_demand=@qty_demand, " +
                                                                                "chksum=@chksum ,dat_creat=@dat_creat where \"id\"=@ID", conB))
                                                                            {
                                                                                cmd2.Parameters.Add("dop", NpgsqlTypes.NpgsqlDbType.Integer);
                                                                                cmd2.Parameters.Add("dop_lin", NpgsqlTypes.NpgsqlDbType.Integer);
                                                                                cmd2.Parameters.Add("data_dop", NpgsqlTypes.NpgsqlDbType.Date);
                                                                                cmd2.Parameters.Add("day_shift", NpgsqlTypes.NpgsqlDbType.Integer);
                                                                                cmd2.Parameters.Add("order_no", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                                                cmd2.Parameters.Add("line_no", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                                                cmd2.Parameters.Add("rel_no", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                                                cmd2.Parameters.Add("int_ord", NpgsqlTypes.NpgsqlDbType.Integer);
                                                                                cmd2.Parameters.Add("contract", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                                                cmd2.Parameters.Add("order_supp_dmd", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                                                cmd2.Parameters.Add("wrkc", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                                                cmd2.Parameters.Add("next_wrkc", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                                                cmd2.Parameters.Add("part_no", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                                                cmd2.Parameters.Add("descr", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                                                cmd2.Parameters.Add("part_code", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                                                cmd2.Parameters.Add("date_required", NpgsqlTypes.NpgsqlDbType.Date);
                                                                                cmd2.Parameters.Add("ord_state", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                                                cmd2.Parameters.Add("ord_date", NpgsqlTypes.NpgsqlDbType.Date);
                                                                                cmd2.Parameters.Add("prod_qty", NpgsqlTypes.NpgsqlDbType.Double);
                                                                                cmd2.Parameters.Add("qty_supply", NpgsqlTypes.NpgsqlDbType.Double);
                                                                                cmd2.Parameters.Add("qty_demand", NpgsqlTypes.NpgsqlDbType.Double);
                                                                                cmd2.Parameters.Add("dat_creat", NpgsqlTypes.NpgsqlDbType.Date);
                                                                                cmd2.Parameters.Add("chksum", NpgsqlTypes.NpgsqlDbType.Integer);
                                                                                cmd2.Parameters.Add("id", NpgsqlTypes.NpgsqlDbType.Uuid);
                                                                                cmd2.Prepare();
                                                                                foreach (DataRow rA in rwA)
                                                                                {
                                                                                    cmd2.Parameters["dop"].Value = Convert.ToInt32(rA["DOP"]);
                                                                                    cmd2.Parameters["dop_lin"].Value = Convert.ToInt32(rA["DOP_LIN"]);
                                                                                    cmd2.Parameters["data_dop"].Value = rA["DATA_DOP"] ?? DBNull.Value; ;
                                                                                    cmd2.Parameters["day_shift"].Value = Convert.ToInt32(rA["DAY_SHIFT"]);
                                                                                    cmd2.Parameters["order_no"].Value = (string)rA["ORDER_NO"];
                                                                                    cmd2.Parameters["line_no"].Value = (string)rA["LINE_NO"];
                                                                                    cmd2.Parameters["rel_no"].Value = (string)rA["REL_NO"];
                                                                                    cmd2.Parameters["int_ord"].Value = Convert.ToInt32(rA["INT_ORD"]);
                                                                                    cmd2.Parameters["contract"].Value = (string)rA["CONTRACT"];
                                                                                    cmd2.Parameters["order_supp_dmd"].Value = (string)rA["ORDER_SUPPLY_DEMAND_TYPE"];
                                                                                    cmd2.Parameters["wrkc"].Value = (string)rA["WRKC"];
                                                                                    cmd2.Parameters["next_wrkc"].Value = (string)rA["NEXT_WRKC"];
                                                                                    cmd2.Parameters["part_no"].Value = (string)rA["PART_NO"];
                                                                                    cmd2.Parameters["descr"].Value = (string)rA["DESCR"];
                                                                                    cmd2.Parameters["part_code"].Value = (string)rA["PART_CODE"];
                                                                                    cmd2.Parameters["date_required"].Value = rA["DATE_REQUIRED"] ?? DBNull.Value; ;
                                                                                    cmd2.Parameters["ord_state"].Value = (string)rA["ORD_STATE"];
                                                                                    cmd2.Parameters["ord_date"].Value = rA["ORD_DATE"] ?? DBNull.Value; ;
                                                                                    cmd2.Parameters["prod_qty"].Value = Convert.ToDouble(rA["PROD_QTY"]);
                                                                                    cmd2.Parameters["qty_supply"].Value = Convert.ToDouble(rA["QTY_SUPPLY"]);
                                                                                    cmd2.Parameters["qty_demand"].Value = Convert.ToDouble(rA["QTY_DEMAND"]);
                                                                                    cmd2.Parameters["dat_creat"].Value = rA["CREAT_DATE"] ?? DBNull.Value; ;
                                                                                    cmd2.Parameters["chksum"].Value = Convert.ToInt32(rA["CHKSUM"]);
                                                                                    cmd2.Parameters["id"].Value = (Guid)rA["ID"];
                                                                                    cmd2.ExecuteNonQuery();
                                                                                }
                                                                                //                 Log("END MODIFY DOP_ord for part3:" + (DateTime.Now - start));
                                                                            }
                                                                            TR_updt.Commit();
                                                                        }
                                                                    }
                                                                    // DODAJ dane
                                                                    rwA = tmp_rows.Select("stat='ADD'");
                                                                    //        Log("RECORDS DOP_ord add3: " + rwA.Length);
                                                                    if (rwA.Length > 0)
                                                                    {
                                                                        using (NpgsqlTransaction TR_insert = conB.BeginTransaction())
                                                                        {
                                                                            using (NpgsqlCommand cmd2 = new NpgsqlCommand("" +
                                                                                "INSERT INTO public.ord_demands" +
                                                                                "(dop, dop_lin, data_dop, day_shift, order_no, line_no, rel_no, int_ord ,contract, order_supp_dmd, wrkc, next_wrkc, part_no, descr, part_code, " +
                                                                                "date_required, ord_state, ord_date, prod_qty, qty_supply, qty_demand,dat_creat , chksum, id) " +
                                                                                "VALUES " +
                                                                                "(@dop,@dop_lin,@data_dop,@day_shift,@order_no,@line_no,@rel_no,@int_ord,@contract,@order_supp_dmd,@wrkc,@next_wrkc,@part_no,@descr,@part_code," +
                                                                                "@date_required,@ord_state,@ord_date,@prod_qty,@qty_supply,@qty_demand,@dat_creat,@chksum,@id);", conB))
                                                                            {
                                                                                cmd2.Parameters.Add("dop", NpgsqlTypes.NpgsqlDbType.Integer);
                                                                                cmd2.Parameters.Add("dop_lin", NpgsqlTypes.NpgsqlDbType.Integer);
                                                                                cmd2.Parameters.Add("data_dop", NpgsqlTypes.NpgsqlDbType.Date);
                                                                                cmd2.Parameters.Add("day_shift", NpgsqlTypes.NpgsqlDbType.Integer);
                                                                                cmd2.Parameters.Add("order_no", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                                                cmd2.Parameters.Add("line_no", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                                                cmd2.Parameters.Add("rel_no", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                                                cmd2.Parameters.Add("int_ord", NpgsqlTypes.NpgsqlDbType.Integer);
                                                                                cmd2.Parameters.Add("contract", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                                                cmd2.Parameters.Add("order_supp_dmd", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                                                cmd2.Parameters.Add("wrkc", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                                                cmd2.Parameters.Add("next_wrkc", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                                                cmd2.Parameters.Add("part_no", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                                                cmd2.Parameters.Add("descr", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                                                cmd2.Parameters.Add("part_code", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                                                cmd2.Parameters.Add("date_required", NpgsqlTypes.NpgsqlDbType.Date);
                                                                                cmd2.Parameters.Add("ord_state", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                                                cmd2.Parameters.Add("ord_date", NpgsqlTypes.NpgsqlDbType.Date);
                                                                                cmd2.Parameters.Add("prod_qty", NpgsqlTypes.NpgsqlDbType.Double);
                                                                                cmd2.Parameters.Add("qty_supply", NpgsqlTypes.NpgsqlDbType.Double);
                                                                                cmd2.Parameters.Add("qty_demand", NpgsqlTypes.NpgsqlDbType.Double);
                                                                                cmd2.Parameters.Add("dat_creat", NpgsqlTypes.NpgsqlDbType.Date);
                                                                                cmd2.Parameters.Add("chksum", NpgsqlTypes.NpgsqlDbType.Integer);
                                                                                cmd2.Parameters.Add("id", NpgsqlTypes.NpgsqlDbType.Uuid);
                                                                                cmd2.Prepare();
                                                                                foreach (DataRow rA in rwA)
                                                                                {
                                                                                    cmd2.Parameters["dop"].Value = Convert.ToInt32(rA["DOP"]);
                                                                                    cmd2.Parameters["dop_lin"].Value = Convert.ToInt32(rA["DOP_LIN"]);
                                                                                    cmd2.Parameters["data_dop"].Value = rA["DATA_DOP"] ?? DBNull.Value; ;
                                                                                    cmd2.Parameters["day_shift"].Value = Convert.ToInt32(rA["DAY_SHIFT"]);
                                                                                    cmd2.Parameters["order_no"].Value = (string)rA["ORDER_NO"];
                                                                                    cmd2.Parameters["line_no"].Value = (string)rA["LINE_NO"];
                                                                                    cmd2.Parameters["rel_no"].Value = (string)rA["REL_NO"];
                                                                                    cmd2.Parameters["int_ord"].Value = Convert.ToInt32(rA["INT_ORD"]);
                                                                                    cmd2.Parameters["contract"].Value = (string)rA["CONTRACT"];
                                                                                    cmd2.Parameters["order_supp_dmd"].Value = (string)rA["ORDER_SUPPLY_DEMAND_TYPE"];
                                                                                    cmd2.Parameters["wrkc"].Value = (string)rA["WRKC"];
                                                                                    cmd2.Parameters["next_wrkc"].Value = (string)rA["NEXT_WRKC"];
                                                                                    cmd2.Parameters["part_no"].Value = (string)rA["PART_NO"];
                                                                                    cmd2.Parameters["descr"].Value = (string)rA["DESCR"];
                                                                                    cmd2.Parameters["part_code"].Value = (string)rA["PART_CODE"];
                                                                                    cmd2.Parameters["date_required"].Value = rA["DATE_REQUIRED"] ?? DBNull.Value; ;
                                                                                    cmd2.Parameters["ord_state"].Value = (string)rA["ORD_STATE"];
                                                                                    cmd2.Parameters["ord_date"].Value = rA["ORD_DATE"] ?? DBNull.Value; ;
                                                                                    cmd2.Parameters["prod_qty"].Value = Convert.ToDouble(rA["PROD_QTY"]);
                                                                                    cmd2.Parameters["qty_supply"].Value = Convert.ToDouble(rA["QTY_SUPPLY"]);
                                                                                    cmd2.Parameters["qty_demand"].Value = Convert.ToDouble(rA["QTY_DEMAND"]);
                                                                                    cmd2.Parameters["dat_creat"].Value = rA["CREAT_DATE"] ?? DBNull.Value; ;
                                                                                    cmd2.Parameters["chksum"].Value = Convert.ToInt32(rA["CHKSUM"]);
                                                                                    cmd2.Parameters["id"].Value = System.Guid.NewGuid();
                                                                                    cmd2.ExecuteNonQuery();
                                                                                }
                                                                                //                  Log("END ADD DOP_ord for part3:" + (DateTime.Now - start));
                                                                            }
                                                                            TR_insert.Commit();
                                                                        }
                                                                    }
                                                                    if (era_sourc.Rows.Count > 0)
                                                                    {
                                                                        using (NpgsqlTransaction TR_del = conB.BeginTransaction())
                                                                        {
                                                                            using (NpgsqlCommand cmd2 = new NpgsqlCommand("delete from ord_demands where \"id\"=@id", conB))
                                                                            {
                                                                                cmd2.Parameters.Add("@id", NpgsqlTypes.NpgsqlDbType.Uuid);
                                                                                cmd2.Prepare();
                                                                                foreach (DataRowView rA in era_sourc.DefaultView)
                                                                                {
                                                                                    cmd2.Parameters[0].Value = rA["id"];
                                                                                    cmd2.ExecuteNonQuery();
                                                                                }
                                                                                //                Log("ERASE dont match DOP_ord for part3" + (DateTime.Now - start));
                                                                            }
                                                                            TR_del.Commit();
                                                                        }
                                                                    }
                                                                }
                                                                catch (Exception e)
                                                                {
                                                                    Log("Błąd modyfikacji tabeli ord_dmd3 postegrsql:" + e);
                                                                }

                                                            }
                                                        }
                                                    }
                                                    //     Log("read get_ord for part3:" + part_no + " Time:" + (DateTime.Now - start));
                                                }
                                            }
                                            part_no = (string)row["part_no"];
                                            first = (DateTime)row["Dat"];
                                            last = (DateTime)row["Dat"];
                                        }
                                        last = (DateTime)row["Dat"];
                                        dt_len++;
                                    }
                                }
                            }
                            using (NpgsqlCommand cmd1 = new NpgsqlCommand("" +
                                "UPDATE public.datatbles " +
                                "SET last_modify=current_timestamp, in_progress=false,updt_errors=false " +
                                "WHERE table_name='worker4'", conB))
                            {
                                cmd1.ExecuteNonQuery();
                            }
                        }
                    }
                }
                GC.Collect();
                Log("End WORKER4");
                return 0;
            }
            catch (Exception e)
            {
                Log("Błąd odczytu informacji z ORACLE :" + e);
                return 1;
            }

        }
        private async Task<int> Get_ord_dem4()
        {
            try
            {
                using (OracleConnection conO = new OracleConnection("Password = pass;User ID = user; Data Source = prod8"))
                {
                    await conO.OpenAsync();
                    using (NpgsqlConnection conB = new NpgsqlConnection(npA))
                    {
                        conB.Open();
                        using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                            "UPDATE public.datatbles " +
                            "SET start_update=current_timestamp, in_progress=true " +
                            "WHERE table_name='worker5'", conB))
                        {
                            cmd.ExecuteNonQuery();
                        }
                        Log("start get_ord4:" + (DateTime.Now - start));
                        using (OracleCommand cmd = new OracleCommand("" +
                            "SELECT  a.DOP,Nvl(a.LINE_ITEM_NO,a.DOP_LIN) DOP_LIN,Nvl(ifsapp.dop_order_api.Get_Revised_Due_Date(a.DOP,1),a.DATE_REQUIRED) Data_dop," +
                            "Nvl(ifsapp.work_time_calendar_api.Get_Work_Days_Between(ifsapp.site_api.Get_Manuf_Calendar_Id(a.CONTRACT), ifsapp.dop_order_api.Get_Revised_Start_Date(a.DOP, a.DOP_LIN), " +
                            "ifsapp.dop_order_api.Get_Revised_Due_Date(a.DOP, 1)) + ifsapp.dop_order_api.Dop_Order_Slack(a.DOP, a.DOP_LIN) + 1, 0) DAy_shift,a.ORDER_NO, a.LINE_NO, a.REL_NO," +
                            "cast(Decode(LENGTH(TRIM(TRANSLATE(a.ORDER_NO, ' +-.0123456789',' '))),NULL,a.ORDER_NO,owa_opt_lock.checksum(a.ORDER_NO)*-1) as INT) int_ord, a.CONTRACT, a.ORDER_SUPPLY_DEMAND_TYPE, " +
                            "a.WRKC, a.NEXT_WRKC, a.PART_NO, ifsapp.inventory_part_api.Get_Description(CONTRACT, a.PART_NO) Descr, a.PART_CODE, a.DATE_REQUIRED,a.ORD_STATE, a.ORD_DATE, cast(a.PROD_QTY as float) PROD_QTY, " +
                            "cast(a.QTY_SUPPLY as float) QTY_SUPPLY, cast(a.QTY_DEMAND as float) QTY_DEMAND,To_Date(creat_date) creat_date, chksum " +
                            "from " +
                                "(SELECT Nvl(ifsapp.dop_supply_shop_ord_api.Get_C_Dop_Id(order_no, line_no, rel_no), 0) DOP," +
                                "Nvl(REPLACE(SubStr(ifsapp.shop_ord_api.Get_Source(order_no, line_no, rel_no), InStr(ifsapp.shop_ord_api.Get_Source(order_no, line_no, rel_no), '^', 10)), '^', ''), 0) DOP_LIN," +
                                "order_no, line_no, rel_no, LINE_ITEM_NO, ifsapp.shop_ord_api.Get_Part_No(order_no, line_no, rel_no) Cust_part_no, CONTRACT, " +
                                "ORDER_SUPPLY_DEMAND_TYPE," +
                                "ifsapp.shop_order_operation_list_api.Get_Next_Op_Work_Center(order_no, line_no, rel_no, ifsapp.shop_order_operation_list_api.Get_Prev_Non_Parallel_Op(order_no, line_no, rel_no, 1, 0)) WRKC," +
                                "ifsapp.shop_order_operation_list_api.Get_Next_Op_Work_Center(order_no, line_no, rel_no, ifsapp.shop_order_operation_list_api.Get_Prev_Non_Parallel_Op(order_no, line_no, rel_no, 2, 0)) NEXT_WRKC," +
                                "PART_NO, STATUS_CODE PART_CODE, DATE_REQUIRED, ifsapp.shop_ord_api.Get_State(order_no, line_no, rel_no) ORD_STATE, ifsapp.shop_ord_api.Get_Revised_Due_Date(order_no, line_no, rel_no) ORD_date," +
                                "Nvl(ifsapp.shop_ord_api.Get_Revised_Qty_Due(order_no, line_no, rel_no)-ifsapp.shop_ord_api.Get_Qty_Complete(order_no, line_no, rel_no), 0) Prod_QTY,0 QTY_SUPPLY, QTY_DEMAND, " +
                                "ifsapp.shop_ord_api.Get_Date_Entered(order_no, line_no, rel_no) creat_date, owa_opt_lock.checksum(ROWID||QTY_DEMAND||QTY_PEGGED||QTY_RESERVED||" +
                                    "To_Char(ifsapp.shop_order_operation_api.Get_Op_Start_Date(order_no,line_no,rel_no,ifsapp.shop_order_operation_list_api.Get_Prev_Non_Parallel_Op (order_no,line_no,rel_no,2,0)),'YYYYMMDDHH24miss')) chksum  " +
                               "FROM ifsapp.shop_material_alloc_demand WHERE part_no = :part_no AND DATE_REQUIRED between :Dat and :Dat1 " +
                               "UNION ALL " +
                               "sELECT To_Number(ORDER_NO) DOP, LINE_NO DOP_LIN, '0' ORDER_NO, '*' LINE_NO, '*' REL_NO, LINE_ITEM_NO, NULL Cust_part_no, CONTRACT, ORDER_SUPPLY_DEMAND_TYPE, ' ' WRKC, ' ' NEXT_WRKC, PART_NO," +
                               " STATUS_CODE PART_CODE,DATE_REQUIRED, ifsapp.dop_head_api.Get_Status(ORDER_NO) ORD_STATE, ifsapp.dop_head_api.Get_Due_Date(ORDER_NO) ORD_DATE, " +
                               "ifsapp.dop_head_api.Get_Qty_Demand(order_no) PROD_QTY, 0 QTY_SUPPLY, QTY_DEMAND, NULL creat_date,owa_opt_lock.checksum(order_no||QTY_DEMAND||DATE_REQUIRED||ORDER_NO||LINE_NO||INFO) chksum  " +
                               "FROM ifsapp.dop_order_demand_ext " +
                               "WHERE part_no = :part_no AND DATE_REQUIRED between :Dat and :Dat1 " +
                               "UNION ALL " +
                               "SELECT ifsapp.customer_order_line_api.Get_Pre_Accounting_Id(ORDER_NO, LINE_NO, REL_NO, LINE_ITEM_NO) DOP, '0' DOP_LIN, ORDER_NO, LINE_NO, REL_NO, LINE_ITEM_NO, PART_NO Cust_part_no, CONTRACT, " +
                               "ORDER_SUPPLY_DEMAND_TYPE, ' ' WRKC, ' ' NEXT_WRKC, PART_NO, STATUS_CODE PART_CODE, DATE_REQUIRED,STATUS_CODE ORD_STATE, DATE_REQUIRED ORD_DATE, 0 PROD_QTY, 0 QTY_SUPPLY, QTY_DEMAND, " +
                               "ifsapp.customer_order_line_api.Get_Date_Entered(order_no, line_no, rel_no, line_item_no) creat_date, owa_opt_lock.checksum(ROW_ID||QTY_DEMAND||DATE_REQUIRED||QTY_PEGGED||QTY_RESERVED) chksum " +
                               "FROM " +
                               "ifsapp.customer_order_line_demand_oe " +
                               "WHERE part_no = :part_no AND DATE_REQUIRED between :Dat and :Dat1 " +
                               "UNION ALL " +
                               "SELECT 0 DOP, '0' DOP_LIN, a.ORDER_NO, a.LINE_NO, a.REL_NO, a.LINE_ITEM_NO, a.PART_NO Cust_part_no, a.CONTRACT, a.ORDER_SUPPLY_DEMAND_TYPE, ' ' WRKC, ' ' NEXT_WRKC, a.PART_NO, a.STATUS_CODE PART_CODE, " +
                               "a.DATE_REQUIRED, a.STATUS_CODE ORD_STATE, a.DATE_REQUIRED ORD_DATE, 0 PROD_QTY, 0 QTY_SUPPLY, a.QTY_DEMAND, b.DATE_ENTERED creat_date, owa_opt_lock.checksum(a.ROWID||QTY_DEMAND||DATE_REQUIRED||a.STATUS_CODE) chksum  " +
                               "FROM ifsapp.material_requis_line_demand_oe a, " +
                               "ifsapp.material_requis_line b " +
                               "WHERE b.OBJID = a.ROW_ID and  a.part_no = :part_no AND a.DATE_REQUIRED between :Dat and :Dat1  " +
                               "UNION ALL  " +
                               "SELECT 0 DOP, '0' DOP_LIN, ORDER_NO, LINE_NO, REL_NO, LINE_ITEM_NO, PART_NO Cust_part_no, CONTRACT, ORDER_SUPPLY_DEMAND_TYPE, ' ' WRKC, ' ' NEXT_WRKC, PART_NO, STATUS_CODE PART_CODE, DATE_REQUIRED, STATUS_CODE ORD_STATE, DATE_REQUIRED ORD_DATE, 0 PROD_QTY, QTY_SUPPLY, 0 QTY_DEMAND, ifsapp.customer_order_line_api.Get_Date_Entered(order_no, line_no, rel_no, line_item_no) creat_date, owa_opt_lock.checksum(ROWID||QTY_SUPPLY||DATE_REQUIRED) chksum FROM ifsapp.purchase_order_line_supply WHERE part_no = :part_no AND DATE_REQUIRED between :Dat and :Dat1  UNION ALL SELECT 0 DOP, '0' DOP_LIN, ORDER_NO, LINE_NO, REL_NO, LINE_ITEM_NO, PART_NO Cust_part_no, CONTRACT, ORDER_SUPPLY_DEMAND_TYPE, ' ' WRKC, ' ' NEXT_WRKC, PART_NO, STATUS_CODE PART_CODE, DATE_REQUIRED, STATUS_CODE ORD_STATE, DATE_REQUIRED ORD_DATE, 0 PROD_QTY, QTY_SUPPLY, 0 QTY_DEMAND, ifsapp.customer_order_line_api.Get_Date_Entered(order_no, line_no, rel_no, line_item_no) creat_date, owa_opt_lock.checksum(ROWID||QTY_SUPPLY||DATE_REQUIRED||STATUS_CODE) chksum FROM ifsapp.ARRIVED_PUR_ORDER_EXT WHERE part_no = :part_no AND DATE_REQUIRED between :Dat and :Dat1 ) a order by to_number(dop) desc,to_number(dop_lin),to_number(int_ord),line_no,rel_no ", conO))
                        {
                            //cmd.FetchSize = cmd.FetchSize * 512;
                            cmd.Parameters.Add(":part_no", OracleDbType.Varchar2);
                            cmd.Parameters.Add(":Dat", OracleDbType.Date);
                            cmd.Parameters.Add(":Dat1", OracleDbType.Date);
                            cmd.Prepare();
                            Log("Prepare command get_ord4:" + (DateTime.Now - start));
                            using (NpgsqlCommand cmd1 = new NpgsqlCommand("" +
                                "select * from public.ord_demands " +
                                "where part_no=@part_no and DATE_REQUIRED between @first and @last " +
                                "order by dop desc,dop_lin,int_ord,LINE_NO,REL_NO;", conB))
                            {
                                cmd1.Parameters.Add("@part_no", NpgsqlTypes.NpgsqlDbType.Varchar);
                                cmd1.Parameters.Add("@first", NpgsqlTypes.NpgsqlDbType.Date);
                                cmd1.Parameters.Add("@last", NpgsqlTypes.NpgsqlDbType.Date);
                                cmd1.Prepare();
                                DataRow[] dt = MShedul.Select("kol = 4");
                                if (dt.Length > 0)
                                {
                                    string part_no = (string)dt[0]["part_no"];
                                    DateTime first = (DateTime)dt[0]["Dat"];
                                    DateTime last = (DateTime)dt[0]["Dat"];
                                    int dt_len = 0;
                                    foreach (DataRow row in dt)
                                    {
                                        if ((string)row["part_no"] != part_no || dt_len == dt.Length - 1)
                                        {
                                            if (first == last) { last = last.AddDays(1); }
                                            cmd.Parameters[0].Value = part_no;
                                            cmd.Parameters[1].Value = first;
                                            cmd.Parameters[2].Value = last;
                                            using (OracleDataReader re = cmd.ExecuteReader())
                                            {
                                                using (DataTable tmp_rows = new DataTable())
                                                {
                                                    tmp_rows.Load(re);
                                                    tmp_rows.Columns.Add("id", System.Type.GetType("System.Guid"));
                                                    tmp_rows.Columns.Add("stat", System.Type.GetType("System.String"));

                                                    //          Log("read get_ord_ORA for part4:" + part_no + " from:" + first + " To:" + last + " Time:" + (DateTime.Now - start));
                                                    cmd1.Parameters[0].Value = part_no;
                                                    cmd1.Parameters[1].Value = first;
                                                    cmd1.Parameters[2].Value = last;
                                                    using (NpgsqlDataReader po1 = cmd1.ExecuteReader())
                                                    {
                                                        using (DataTable sou_rows = new DataTable())
                                                        {
                                                            DataTable sch = po1.GetSchemaTable();
                                                            foreach (DataRow a in sch.Rows)
                                                            {
                                                                sou_rows.Columns.Add(a["ColumnName"].ToString(), System.Type.GetType(a["DataType"].ToString()));
                                                                sou_rows.Columns[a["ColumnName"].ToString()].AllowDBNull = true;
                                                            }
                                                            sch.Clear();
                                                            sch.Dispose();
                                                            sou_rows.Load(po1);
                                                            //tmp_rows.DefaultView.Sort = "dop desc,dop_lin,int_ord,LINE_NO,REL_NO";
                                                            //               Log("read get_ord_postg for part4:" + part_no + " from:" + first + " To:" + last + " Records ORA:" + tmp_rows.Rows.Count + " Records PSTGR:" + sou_rows.Rows.Count + " Time:" + (DateTime.Now - start)); ;
                                                            int ind_sou_rows = 0;
                                                            int max_sou_rows = sou_rows.Rows.Count;
                                                            using (DataTable era_sourc = sou_rows.Clone())
                                                            {
                                                                foreach (DataColumn a in era_sourc.Columns)
                                                                {
                                                                    a.AllowDBNull = true;
                                                                }
                                                                int counter = -1;
                                                                int max = tmp_rows.Rows.Count;
                                                                foreach (DataRowView rw in tmp_rows.DefaultView)
                                                                {
                                                                    if (counter < max) { counter++; }
                                                                    if (sou_rows.Rows.Count > 0 && max_sou_rows > ind_sou_rows)
                                                                    {
                                                                        while (Convert.ToInt32(rw["dop"]) < Convert.ToInt32(sou_rows.DefaultView[ind_sou_rows].Row["dop"]) || (Convert.ToInt32(rw["dop"]) == Convert.ToInt32(sou_rows.DefaultView[ind_sou_rows].Row["dop"]) && (Convert.ToInt32(rw["dop_lin"]) > Convert.ToInt32(sou_rows.DefaultView[ind_sou_rows].Row["dop_lin"]) || Convert.ToInt32(rw["int_ord"]) > Convert.ToInt32(sou_rows.DefaultView[ind_sou_rows].Row["int_ord"]))))
                                                                        {
                                                                            // zlecenia wcześniejsze do usunięcia
                                                                            DataRow r = era_sourc.NewRow();
                                                                            for (int i = 0; i < sou_rows.Columns.Count; i++)
                                                                            {
                                                                                r[i] = sou_rows.DefaultView[ind_sou_rows].Row[i];
                                                                            }
                                                                            era_sourc.Rows.Add(r);
                                                                            ind_sou_rows++;
                                                                            if (max_sou_rows <= ind_sou_rows) { break; }
                                                                        }
                                                                        if (max_sou_rows > ind_sou_rows)
                                                                        {
                                                                            if (Convert.ToInt32(rw["dop"]) == Convert.ToInt32(sou_rows.DefaultView[ind_sou_rows].Row["dop"]) && Convert.ToInt32(rw["dop_lin"]) == Convert.ToInt32(sou_rows.DefaultView[ind_sou_rows].Row["dop_lin"]))
                                                                            {
                                                                                if (Convert.ToInt32(rw["int_ord"]) == Convert.ToInt32(sou_rows.DefaultView[ind_sou_rows].Row["int_ord"]) && (string)rw["line_no"] == (string)sou_rows.DefaultView[ind_sou_rows].Row["line_no"] && (string)rw["rel_no"] == (string)sou_rows.DefaultView[ind_sou_rows].Row["rel_no"])
                                                                                {
                                                                                    //rekord istnieje
                                                                                    bool modif = false;
                                                                                    if (Convert.ToInt32(rw["chksum"]) != Convert.ToInt32(sou_rows.DefaultView[ind_sou_rows].Row["chksum"])) { modif = true; }
                                                                                    if (!modif)
                                                                                    {
                                                                                        for (int i = 2; i < tmp_rows.Columns.Count - 2; i++)
                                                                                        {
                                                                                            bool db_or = false;
                                                                                            bool db_pst = false;
                                                                                            if (sou_rows.DefaultView[ind_sou_rows].Row[i] == DBNull.Value || sou_rows.DefaultView[ind_sou_rows].Row[i] == null) { db_pst = true; }
                                                                                            if (rw[i] == DBNull.Value || rw[i] == null) { db_or = true; }
                                                                                            if (i == 3 || i == 7 || (i > 17 && i < 21))
                                                                                            {
                                                                                                if (Convert.ToDouble(rw[i]) != Convert.ToDouble(sou_rows.DefaultView[ind_sou_rows].Row[i])) { modif = true; }
                                                                                            }
                                                                                            else
                                                                                            {
                                                                                                if (!db_or && !db_pst)
                                                                                                {
                                                                                                    if (rw[i].ToString() != sou_rows.DefaultView[ind_sou_rows].Row[i].ToString()) { modif = true; }
                                                                                                }
                                                                                                else
                                                                                                {
                                                                                                    if (db_or != db_pst) { modif = true; }
                                                                                                }
                                                                                            }
                                                                                            if (modif) { break; }
                                                                                        }
                                                                                    }
                                                                                    if (modif)
                                                                                    {
                                                                                        // nastąpiła modyfikacja
                                                                                        rw["stat"] = "MOD";
                                                                                        rw["id"] = sou_rows.DefaultView[ind_sou_rows].Row["id"];
                                                                                    }
                                                                                    ind_sou_rows++;
                                                                                }
                                                                                else
                                                                                {
                                                                                    // brak rekordu dodaję
                                                                                    rw["stat"] = "ADD";
                                                                                    rw["id"] = System.Guid.NewGuid();
                                                                                }
                                                                            }
                                                                            else
                                                                            {
                                                                                // brak rekordu dodaję
                                                                                rw["stat"] = "ADD";
                                                                                rw["id"] = System.Guid.NewGuid();
                                                                            }
                                                                            if (counter >= max - 1 && max_sou_rows > ind_sou_rows)
                                                                            {
                                                                                while (max_sou_rows > ind_sou_rows)
                                                                                {
                                                                                    DataRow r = era_sourc.NewRow();
                                                                                    for (int i = 0; i < sou_rows.Columns.Count; i++)
                                                                                    {
                                                                                        r[i] = sou_rows.DefaultView[ind_sou_rows].Row[i];
                                                                                    }
                                                                                    era_sourc.Rows.Add(r);
                                                                                    ind_sou_rows++;
                                                                                    if (max_sou_rows <= ind_sou_rows) { break; }
                                                                                }
                                                                            }
                                                                        }
                                                                        else
                                                                        {
                                                                            //rekordy do porównania się skończyły
                                                                            rw["stat"] = "ADD";
                                                                        }
                                                                    }
                                                                    else
                                                                    {
                                                                        // nowe dane jeśli brak tabeli PSTGR
                                                                        rw["stat"] = "ADD";
                                                                    }
                                                                }
                                                                //          Log("END compare get_ord for part4:" + part_no + " Time:" + (DateTime.Now - start));
                                                                //Start update
                                                                try
                                                                {
                                                                    DataRow[] rwA = tmp_rows.Select("stat='MOD'");
                                                                    //          Log("RECORDS DOP_ord mod4: " + rwA.Length);
                                                                    if (rwA.Length > 0)
                                                                    {
                                                                        using (NpgsqlTransaction TR_updt = conB.BeginTransaction())
                                                                        {
                                                                            using (NpgsqlCommand cmd2 = new NpgsqlCommand("" +
                                                                                "UPDATE public.ord_demands " +
                                                                                "SET dop=@dop, dop_lin=@dop_lin, data_dop=@data_dop, day_shift=@day_shift, order_no=@order_no, line_no=@line_no, rel_no=@rel_no,int_ord=@int_ord," +
                                                                                " contract=@contract, order_supp_dmd=@order_supp_dmd, wrkc=@wrkc, next_wrkc=@next_wrkc, part_no=@part_no, descr=@descr, part_code=@part_code," +
                                                                                " date_required=@date_required, ord_state=@ord_state, ord_date=@ord_date, prod_qty=@prod_qty, qty_supply=@qty_supply, qty_demand=@qty_demand," +
                                                                                " chksum=@chksum ,dat_creat=@dat_creat where \"id\"=@ID", conB))
                                                                            {
                                                                                cmd2.Parameters.Add("dop", NpgsqlTypes.NpgsqlDbType.Integer);
                                                                                cmd2.Parameters.Add("dop_lin", NpgsqlTypes.NpgsqlDbType.Integer);
                                                                                cmd2.Parameters.Add("data_dop", NpgsqlTypes.NpgsqlDbType.Date);
                                                                                cmd2.Parameters.Add("day_shift", NpgsqlTypes.NpgsqlDbType.Integer);
                                                                                cmd2.Parameters.Add("order_no", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                                                cmd2.Parameters.Add("line_no", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                                                cmd2.Parameters.Add("rel_no", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                                                cmd2.Parameters.Add("int_ord", NpgsqlTypes.NpgsqlDbType.Integer);
                                                                                cmd2.Parameters.Add("contract", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                                                cmd2.Parameters.Add("order_supp_dmd", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                                                cmd2.Parameters.Add("wrkc", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                                                cmd2.Parameters.Add("next_wrkc", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                                                cmd2.Parameters.Add("part_no", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                                                cmd2.Parameters.Add("descr", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                                                cmd2.Parameters.Add("part_code", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                                                cmd2.Parameters.Add("date_required", NpgsqlTypes.NpgsqlDbType.Date);
                                                                                cmd2.Parameters.Add("ord_state", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                                                cmd2.Parameters.Add("ord_date", NpgsqlTypes.NpgsqlDbType.Date);
                                                                                cmd2.Parameters.Add("prod_qty", NpgsqlTypes.NpgsqlDbType.Double);
                                                                                cmd2.Parameters.Add("qty_supply", NpgsqlTypes.NpgsqlDbType.Double);
                                                                                cmd2.Parameters.Add("qty_demand", NpgsqlTypes.NpgsqlDbType.Double);
                                                                                cmd2.Parameters.Add("dat_creat", NpgsqlTypes.NpgsqlDbType.Date);
                                                                                cmd2.Parameters.Add("chksum", NpgsqlTypes.NpgsqlDbType.Integer);
                                                                                cmd2.Parameters.Add("id", NpgsqlTypes.NpgsqlDbType.Uuid);
                                                                                cmd2.Prepare();
                                                                                foreach (DataRow rA in rwA)
                                                                                {
                                                                                    cmd2.Parameters["dop"].Value = Convert.ToInt32(rA["DOP"]);
                                                                                    cmd2.Parameters["dop_lin"].Value = Convert.ToInt32(rA["DOP_LIN"]);
                                                                                    cmd2.Parameters["data_dop"].Value = rA["DATA_DOP"] ?? DBNull.Value; ;
                                                                                    cmd2.Parameters["day_shift"].Value = Convert.ToInt32(rA["DAY_SHIFT"]);
                                                                                    cmd2.Parameters["order_no"].Value = (string)rA["ORDER_NO"];
                                                                                    cmd2.Parameters["line_no"].Value = (string)rA["LINE_NO"];
                                                                                    cmd2.Parameters["rel_no"].Value = (string)rA["REL_NO"];
                                                                                    cmd2.Parameters["int_ord"].Value = Convert.ToInt32(rA["INT_ORD"]);
                                                                                    cmd2.Parameters["contract"].Value = (string)rA["CONTRACT"];
                                                                                    cmd2.Parameters["order_supp_dmd"].Value = (string)rA["ORDER_SUPPLY_DEMAND_TYPE"];
                                                                                    cmd2.Parameters["wrkc"].Value = (string)rA["WRKC"];
                                                                                    cmd2.Parameters["next_wrkc"].Value = (string)rA["NEXT_WRKC"];
                                                                                    cmd2.Parameters["part_no"].Value = (string)rA["PART_NO"];
                                                                                    cmd2.Parameters["descr"].Value = (string)rA["DESCR"];
                                                                                    cmd2.Parameters["part_code"].Value = (string)rA["PART_CODE"];
                                                                                    cmd2.Parameters["date_required"].Value = rA["DATE_REQUIRED"] ?? DBNull.Value; ;
                                                                                    cmd2.Parameters["ord_state"].Value = (string)rA["ORD_STATE"];
                                                                                    cmd2.Parameters["ord_date"].Value = rA["ORD_DATE"] ?? DBNull.Value; ;
                                                                                    cmd2.Parameters["prod_qty"].Value = Convert.ToDouble(rA["PROD_QTY"]);
                                                                                    cmd2.Parameters["qty_supply"].Value = Convert.ToDouble(rA["QTY_SUPPLY"]);
                                                                                    cmd2.Parameters["qty_demand"].Value = Convert.ToDouble(rA["QTY_DEMAND"]);
                                                                                    cmd2.Parameters["dat_creat"].Value = rA["CREAT_DATE"] ?? DBNull.Value; ;
                                                                                    cmd2.Parameters["chksum"].Value = Convert.ToInt32(rA["CHKSUM"]);
                                                                                    cmd2.Parameters["id"].Value = (Guid)rA["ID"];
                                                                                    cmd2.ExecuteNonQuery();
                                                                                }
                                                                                //                Log("END MODIFY DOP_ord for part4:" + (DateTime.Now - start));
                                                                            }
                                                                            TR_updt.Commit();
                                                                        }
                                                                    }
                                                                    // DODAJ dane
                                                                    rwA = tmp_rows.Select("stat='ADD'");
                                                                    //       Log("RECORDS DOP_ord add4: " + rwA.Length);
                                                                    if (rwA.Length > 0)
                                                                    {
                                                                        using (NpgsqlTransaction TR_insert = conB.BeginTransaction(IsolationLevel.ReadCommitted))
                                                                        {
                                                                            using (NpgsqlCommand cmd2 = new NpgsqlCommand("" +
                                                                                "INSERT INTO public.ord_demands" +
                                                                                "(dop, dop_lin, data_dop, day_shift, order_no, line_no, rel_no, int_ord ,contract, order_supp_dmd, wrkc, next_wrkc, part_no, descr, " +
                                                                                "part_code, date_required, ord_state, ord_date, prod_qty, qty_supply, qty_demand,dat_creat , chksum, id) " +
                                                                                "VALUES " +
                                                                                "(@dop,@dop_lin,@data_dop,@day_shift,@order_no,@line_no,@rel_no,@int_ord,@contract,@order_supp_dmd,@wrkc,@next_wrkc,@part_no,@descr,@part_code," +
                                                                                "@date_required,@ord_state,@ord_date,@prod_qty,@qty_supply,@qty_demand,@dat_creat,@chksum,@id);", conB))
                                                                            {
                                                                                cmd2.Parameters.Add("dop", NpgsqlTypes.NpgsqlDbType.Integer);
                                                                                cmd2.Parameters.Add("dop_lin", NpgsqlTypes.NpgsqlDbType.Integer);
                                                                                cmd2.Parameters.Add("data_dop", NpgsqlTypes.NpgsqlDbType.Date);
                                                                                cmd2.Parameters.Add("day_shift", NpgsqlTypes.NpgsqlDbType.Integer);
                                                                                cmd2.Parameters.Add("order_no", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                                                cmd2.Parameters.Add("line_no", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                                                cmd2.Parameters.Add("rel_no", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                                                cmd2.Parameters.Add("int_ord", NpgsqlTypes.NpgsqlDbType.Integer);
                                                                                cmd2.Parameters.Add("contract", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                                                cmd2.Parameters.Add("order_supp_dmd", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                                                cmd2.Parameters.Add("wrkc", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                                                cmd2.Parameters.Add("next_wrkc", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                                                cmd2.Parameters.Add("part_no", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                                                cmd2.Parameters.Add("descr", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                                                cmd2.Parameters.Add("part_code", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                                                cmd2.Parameters.Add("date_required", NpgsqlTypes.NpgsqlDbType.Date);
                                                                                cmd2.Parameters.Add("ord_state", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                                                cmd2.Parameters.Add("ord_date", NpgsqlTypes.NpgsqlDbType.Date);
                                                                                cmd2.Parameters.Add("prod_qty", NpgsqlTypes.NpgsqlDbType.Double);
                                                                                cmd2.Parameters.Add("qty_supply", NpgsqlTypes.NpgsqlDbType.Double);
                                                                                cmd2.Parameters.Add("qty_demand", NpgsqlTypes.NpgsqlDbType.Double);
                                                                                cmd2.Parameters.Add("dat_creat", NpgsqlTypes.NpgsqlDbType.Date);
                                                                                cmd2.Parameters.Add("chksum", NpgsqlTypes.NpgsqlDbType.Integer);
                                                                                cmd2.Parameters.Add("id", NpgsqlTypes.NpgsqlDbType.Uuid);
                                                                                cmd2.Prepare();
                                                                                foreach (DataRow rA in rwA)
                                                                                {
                                                                                    cmd2.Parameters["dop"].Value = Convert.ToInt32(rA["DOP"]);
                                                                                    cmd2.Parameters["dop_lin"].Value = Convert.ToInt32(rA["DOP_LIN"]);
                                                                                    cmd2.Parameters["data_dop"].Value = rA["DATA_DOP"] ?? DBNull.Value; ;
                                                                                    cmd2.Parameters["day_shift"].Value = Convert.ToInt32(rA["DAY_SHIFT"]);
                                                                                    cmd2.Parameters["order_no"].Value = (string)rA["ORDER_NO"];
                                                                                    cmd2.Parameters["line_no"].Value = (string)rA["LINE_NO"];
                                                                                    cmd2.Parameters["rel_no"].Value = (string)rA["REL_NO"];
                                                                                    cmd2.Parameters["int_ord"].Value = Convert.ToInt32(rA["INT_ORD"]);
                                                                                    cmd2.Parameters["contract"].Value = (string)rA["CONTRACT"];
                                                                                    cmd2.Parameters["order_supp_dmd"].Value = (string)rA["ORDER_SUPPLY_DEMAND_TYPE"];
                                                                                    cmd2.Parameters["wrkc"].Value = (string)rA["WRKC"];
                                                                                    cmd2.Parameters["next_wrkc"].Value = (string)rA["NEXT_WRKC"];
                                                                                    cmd2.Parameters["part_no"].Value = (string)rA["PART_NO"];
                                                                                    cmd2.Parameters["descr"].Value = (string)rA["DESCR"];
                                                                                    cmd2.Parameters["part_code"].Value = (string)rA["PART_CODE"];
                                                                                    cmd2.Parameters["date_required"].Value = rA["DATE_REQUIRED"] ?? DBNull.Value; ;
                                                                                    cmd2.Parameters["ord_state"].Value = (string)rA["ORD_STATE"];
                                                                                    cmd2.Parameters["ord_date"].Value = rA["ORD_DATE"] ?? DBNull.Value; ;
                                                                                    cmd2.Parameters["prod_qty"].Value = Convert.ToDouble(rA["PROD_QTY"]);
                                                                                    cmd2.Parameters["qty_supply"].Value = Convert.ToDouble(rA["QTY_SUPPLY"]);
                                                                                    cmd2.Parameters["qty_demand"].Value = Convert.ToDouble(rA["QTY_DEMAND"]);
                                                                                    cmd2.Parameters["dat_creat"].Value = rA["CREAT_DATE"] ?? DBNull.Value; ;
                                                                                    cmd2.Parameters["chksum"].Value = Convert.ToInt32(rA["CHKSUM"]);
                                                                                    cmd2.Parameters["id"].Value = System.Guid.NewGuid();
                                                                                    cmd2.ExecuteNonQuery();
                                                                                }
                                                                                //              Log("END ADD DOP_ord for part4:" + (DateTime.Now - start));
                                                                            }
                                                                            TR_insert.Commit();
                                                                        }
                                                                    }
                                                                    if (era_sourc.Rows.Count > 0)
                                                                    {
                                                                        using (NpgsqlTransaction TR_delt = conB.BeginTransaction())
                                                                        {
                                                                            using (NpgsqlCommand cmd2 = new NpgsqlCommand("delete from ord_demands where \"id\"=@id", conB))
                                                                            {
                                                                                cmd2.Parameters.Add("@id", NpgsqlTypes.NpgsqlDbType.Uuid);
                                                                                cmd2.Prepare();
                                                                                foreach (DataRowView rA in era_sourc.DefaultView)
                                                                                {
                                                                                    cmd2.Parameters[0].Value = rA["id"];
                                                                                    cmd2.ExecuteNonQuery();
                                                                                }
                                                                                //           Log("ERASE dont match DOP_ord for part4" + (DateTime.Now - start));
                                                                            }
                                                                            TR_delt.Commit();
                                                                        }
                                                                    }
                                                                }
                                                                catch (Exception e)
                                                                {
                                                                    Log("Błąd modyfikacji tabeli ord_dmd4 postegrsql:" + e);
                                                                }
                                                            }
                                                        }
                                                    }
                                                    //         Log("read get_ord for part4:" + part_no + " Time:" + (DateTime.Now - start));
                                                }
                                            }
                                            part_no = (string)row["part_no"];
                                            first = (DateTime)row["Dat"];
                                            last = (DateTime)row["Dat"];
                                        }
                                        last = (DateTime)row["Dat"];
                                        dt_len++;
                                    }
                                }
                            }
                            using (NpgsqlCommand cmd1 = new NpgsqlCommand("" +
                                "UPDATE public.datatbles " +
                                "SET last_modify=current_timestamp, in_progress=false,updt_errors=false " +
                                "WHERE table_name='worker5'", conB))
                            {
                                cmd1.ExecuteNonQuery();
                            }
                        }
                    }
                }
                GC.Collect();
                Log("End WORKER5");
                return 0;
            }
            catch (Exception e)
            {
                Log("Błąd odczytu informacji z ORACLE :" + e);
                return 1;
            }
        }
        private async Task<int> Calculate_cust_ord()
        {
            try
            {
                using (NpgsqlConnection conA = new NpgsqlConnection(npA))
                {
                    conA.Open();
                    using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                        "select cast(count(table_name) as integer) busy " +
                        "from public.datatbles " +
                        "where (substring(table_name,1,6)='worker' or table_name='demands'  or table_name='wrk_del' or  table_name='data' or table_name='cust_ord') and in_progress=true", conA))
                    {
                        int busy_il = 1;
                        while (busy_il > 0)
                        {
                            busy_il = Convert.ToInt16(cmd.ExecuteScalar());
                            if (busy_il > 0) { System.Threading.Thread.Sleep(250); }
                        }
                    }
                }
                using (NpgsqlConnection conA = new NpgsqlConnection(npC))
                {
                    conA.Open();
                    using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                        "UPDATE public.datatbles	" +
                        "SET start_update=current_timestamp, in_progress=true " +
                        "WHERE substr(table_name,1,5)='braki' or substring(table_name,1,7)='cal_ord'", conA))
                    {
                        cmd.ExecuteNonQuery();
                    }
                }
                Log("Start query for problematic orders Time:" + (DateTime.Now - start));
                using (DataTable mat_ord = new DataTable())
                {
                    using (DataTable mat_dmd = new DataTable())
                    {
                        using (NpgsqlConnection conA = new NpgsqlConnection(npA))
                        {
                            conA.Open();
                            using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                                "select c.indeks,c.mag,count(case when order_supp_dmd!='Zam. zakupu' then a.part_no end) il," +
                                "case when c.typ_zdarzenia='Braki w gwarantowanej dacie' then c.data_gwarancji else c.data_dost end data_dost,c.wlk_dost,c.sum_dost,c.bilans," +
                                "c.mag-sum(a.qty_demand)+case when c.typ_zdarzenia in ('Dzisiejsza dostawa','Opóźniona dostawa') then 0 else sum(a.qty_supply) end bil_chk," +
                                "c.typ_zdarzenia,e.max_prod_date,sum(a.qty_supply) dost,sum(a.qty_demand) potrz,sum_dost-sum_dost qty " +
                                "from " +
                                    "(select c.indeks,c.data_dost,max(date_shift_days(case when  c.typ_zdarzenia='Braki w gwarantowanej dacie' then c.data_gwarancji else c.data_dost end,a.day_shift)) as max_prod_date " +
                                    "from " +
                                        "(select * " +
                                        "from public.data " +
                                        "where typ_zdarzenia not in ('Brak zamówień zakupu','Dostawa na dzisiejsze ilości') and planner_buyer!='LUCPRZ')c," +
                                        "ord_demands a " +
                                    "where c.bilans<0 and a.part_no=c.indeks and (a.date_required<=c.data_dost or a.date_required<=current_date) group by indeks,data_dost) e," +
                                    "(select * " +
                                    "from public.data " +
                                    "where typ_zdarzenia not in ('Brak zamówień zakupu','Dostawa na dzisiejsze ilości') and planner_buyer!='LUCPRZ')c," +
                                    "ord_demands a " +
                                    "left join " +
                                    "cust_ord b " +
                                    "on b.dop_id=a.dop " +
                                    "where c.bilans<0 and a.part_no=c.indeks and e.indeks=c.indeks " +
                                    "and ((c.typ_zdarzenia='Brakujące ilości' and a.date_required<c.data_dost) or (c.typ_zdarzenia!='Brakujące ilości' and a.date_required<=c.data_dost) " +
                                        "or a.date_required<=current_date) and e.data_dost=c.data_dost " +
                                    "group by c.indeks,c.mag,c.data_gwarancji,c.data_dost,c.wlk_dost,c.sum_dost,c.bilans,c.typ_zdarzenia,e.max_prod_date " +
                                    "order by max_prod_date desc,c.indeks,data_dost desc ", conA))
                            {
                                using (NpgsqlDataReader po = cmd.ExecuteReader())
                                {
                                    mat_dmd.Load(po);
                                    Log("Balanace for materials:" + (DateTime.Now - start));
                                }
                            }
                            using (NpgsqlCommand cmd = new NpgsqlCommand("select case when a.dop=0 then 'O '||a.order_no else 'D'||to_char(a.dop,'9999999999') end ordID,f.l_ordid ,c.indeks,c.opis,c.planner_buyer,c.mag, c.data_dost ,a.date_required,c.wlk_dost,c.bilans,c.typ_zdarzenia,c.status_informacji,a.dop,a.dop_lin,a.data_dop,a.order_no zlec,date_shift_days(case when  c.typ_zdarzenia='Braki w gwarantowanej dacie' then c.data_gwarancji else c.data_dost end,a.day_shift) as prod_date,to_date(b.prom_week, 'iyyyiw')+shipment_day(b.country,b.cust_no,b.zip_code,b.addr1)-2 as max_posible_prod,e.max_prod_date,a.order_supp_dmd,a.part_code,a.ord_state,a.prod_qty,a.qty_supply,a.qty_demand,b.koor,b.order_no,b.line_no,b.rel_no,b.part_no,b.descr,b.configuration,b.last_mail_conf,b.prom_date,b.prom_week,b.load_id,b.ship_date,b.state_conf,b.line_state,b.cust_order_state,b.country,shipment_day(b.country,b.cust_no,b.zip_code,b.addr1),COALESCE (b.date_entered,a.dat_creat) as date_entered,(case when to_date(b.prom_week, 'iyyyiw')+shipment_day(b.country,b.cust_no,b.zip_code,b.addr1)-2<date_shift_days(c.data_dost,a.day_shift) then COALESCE (b.date_entered,a.dat_creat) else  COALESCE (b.date_entered,a.dat_creat,'2014-01-01')+ interval '200 day' end) + cast(right(cast(b.custid as varchar),4)||' microseconds' as interval)  as sort_ord,b.zest,qty_demand-qty_demand ord_assinged,b.id custid from (select c.indeks,c.data_dost,max(date_shift_days(case when  c.typ_zdarzenia='Braki w gwarantowanej dacie' then c.data_gwarancji else c.data_dost end,a.day_shift)) as max_prod_date,count(case when a.dop=0 then 'O '||a.order_no else 'D'||to_char(a.dop,'9999999999') end) from (select * from public.data where typ_zdarzenia not in ('Brak zamówień zakupu','Dostawa na dzisiejsze ilości') and planner_buyer!='LUCPRZ')c,ord_demands a where c.bilans<0 and a.part_no=c.indeks and (a.date_required<=c.data_dost or a.date_required<=current_date) group by indeks,data_dost) e,(select case when a.dop=0 then 'O '||a.order_no else 'D'||to_char(a.dop,'9999999999') end ordid,count(case when a.dop=0 then 'O '||a.order_no else 'D'||to_char(a.dop,'9999999999') end) l_ordid from (select * from public.data where typ_zdarzenia not in ('Brak zamówień zakupu','Dostawa na dzisiejsze ilości') and planner_buyer!='LUCPRZ')c,ord_demands a where c.bilans<0 and a.part_no=c.indeks and (a.date_required<=c.data_dost or a.date_required<=current_date) group by case when a.dop=0 then 'O '||a.order_no else 'D'||to_char(a.dop,'9999999999') end) f,(select * from public.data where typ_zdarzenia not in ('Brak zamówień zakupu','Dostawa na dzisiejsze ilości') and planner_buyer!='LUCPRZ')c,ord_demands a left join cust_ord b on b.dop_id=a.dop where a.order_supp_dmd!='Zam. zakupu' and f.ordid=case when a.dop=0 then 'O '||a.order_no else 'D'||to_char(a.dop,'9999999999') end and c.bilans<0 and a.part_no=c.indeks and e.indeks=c.indeks and (case when typ_zdarzenia='Brakujące ilości' then a.date_required<c.data_dost else a.date_required<=c.data_dost end or a.date_required<=current_date) and e.data_dost=c.data_dost order by max_prod_date desc,c.indeks,data_dost,sort_ord desc", conA))
                            {
                                using (NpgsqlDataReader po = cmd.ExecuteReader())
                                {
                                    using (DataTable sch = po.GetSchemaTable())
                                    {
                                        foreach (DataRow a in sch.Rows)
                                        {
                                            mat_ord.Columns.Add(a["ColumnName"].ToString(), System.Type.GetType(a["DataType"].ToString()));
                                            mat_ord.Columns[a["ColumnName"].ToString()].AllowDBNull = true;
                                        }
                                    }
                                    mat_ord.Load(po);
                                    Log("Data for Balanace for orders:" + (DateTime.Now - start));
                                }
                            }
                        }
                        using (DataTable zes = new DataTable())
                        {
                            zes.Columns.Add("zest_id", System.Type.GetType("System.String"));
                            zes.Columns.Add("ilo", System.Type.GetType("System.Int32"));
                            string zst = "";
                            foreach (DataRow qwe in mat_ord.Rows)
                            {
                                if (!qwe.IsNull("zest"))
                                {
                                    if (zst.IndexOf("'Zes" + qwe["zest"].ToString() + "'") == -1)
                                    {
                                        zst = zst + "'Zes" + qwe["zest"].ToString() + "'";
                                        DataRow rek = zes.NewRow();
                                        rek["zest_id"] = qwe["zest"];
                                        rek["ilo"] = 1;
                                        zes.Rows.Add(rek);
                                    }
                                    else
                                    {
                                        foreach (DataRow rw in zes.Rows)
                                        {
                                            if (rw["zest_id"] == qwe["zest"])
                                            {
                                                rw["ilo"] = Convert.ToDecimal(rw["ilo"]) + 1;
                                                rw.AcceptChanges();
                                                break;
                                            }
                                        }
                                    }
                                }
                            }
                            List<DataRow> rw_delete = new List<DataRow>();
                            foreach (DataRow rw in zes.Rows)
                            {
                                if (Convert.ToDecimal(rw["ilo"]) == 1)
                                {
                                    zst.Replace("'Zes" + rw["zest_id"].ToString() + "'", "");
                                    rw_delete.Add(rw);
                                }
                            }
                            foreach (DataRow rw in rw_delete)
                            {
                                zes.Rows.Remove(rw);
                            }
                            zes.AcceptChanges();
                            using (DataTable tmp_ord = mat_ord.Clone())
                            {
                                //przypisz zmienne
                                string isfull = "";

                                Log("Calculate balance:" + (DateTime.Now - start));
                                for (int j = 0; j < mat_dmd.Rows.Count; j++)
                                {
                                    DataTable ord_ids = zes.Clone();
                                    DataRow rek = mat_dmd.Rows[j];
                                    string sear_id = "";
                                    int iter = Convert.ToInt32(rek["il"]);
                                    double qt = Convert.ToDouble(rek["bil_chk"]);
                                    double bil = Convert.ToDouble(rek["qty"]);
                                    if (bil > qt)
                                    {
                                        for (int i = 0; i < iter; i++)
                                        {
                                            DataRow rw = mat_ord.Rows[i];
                                            if (rek["indeks"].ToString() == rw["indeks"].ToString())
                                            {
                                                if (Convert.ToDouble(rw["ord_assinged"]) == 0)
                                                {
                                                    if (bil > qt)
                                                    {
                                                        if (rw["ord_state"].ToString() != "Rozpoczęte")
                                                        {
                                                            if (Convert.ToDecimal(rw["l_ordid"]) > 1)
                                                            {
                                                                if (sear_id.IndexOf("'" + rw["ordid"].ToString() + "'") == -1)
                                                                {
                                                                    DataRow row = ord_ids.NewRow();
                                                                    row[0] = rw["ordid"];
                                                                    row[1] = rw["l_ordid"];
                                                                    ord_ids.Rows.Add(row);
                                                                    sear_id = sear_id + "'" + rw["ordid"].ToString() + "'";
                                                                }
                                                                else
                                                                {
                                                                    DataRow[] strf = ord_ids.Select("zest_id='" + rw["ordid"].ToString() + "'");
                                                                    if (strf.Length > 0)
                                                                    {
                                                                        foreach (DataRow ra in strf)
                                                                        {
                                                                            if (Convert.ToDecimal(ra["ilo"]) == 1) { sear_id.Replace("'" + ra["zest_id"] + "'", ""); }
                                                                            ra["ilo"] = Convert.ToDecimal(ra["ilo"]) - 1;
                                                                            ra.AcceptChanges();
                                                                        }
                                                                    }
                                                                }
                                                            }
                                                            if (!rw.IsNull("zest"))
                                                            {
                                                                if (sear_id.IndexOf("'Zes" + rw["zest"].ToString() + "'") == -1)
                                                                {
                                                                    if (zst.IndexOf("'Zes" + rw["zest"].ToString() + "'") > -1)
                                                                    {
                                                                        foreach (DataRow re in zes.Rows)
                                                                        {
                                                                            if (re["zest_id"] == rw["zest"])
                                                                            {
                                                                                DataRow row = ord_ids.NewRow();
                                                                                row[0] = re[0];
                                                                                row[1] = re[1];
                                                                                ord_ids.Rows.Add(row);
                                                                                break;
                                                                            }
                                                                        }
                                                                        sear_id = sear_id + "'Zes" + rw["zest"].ToString() + "'";
                                                                    }
                                                                }
                                                                else
                                                                {
                                                                    DataRow[] strf = ord_ids.Select("zest_id='Zes" + rw["zest"].ToString() + "'");
                                                                    if (strf.Length > 0)
                                                                    {
                                                                        foreach (DataRow ra in strf)
                                                                        {
                                                                            if (Convert.ToDecimal(ra["ilo"]) == 1) { sear_id.Replace("'" + ra["zest_id"] + "'", ""); }
                                                                            ra["ilo"] = Convert.ToDecimal(ra["ilo"]) - 1;
                                                                            ra.AcceptChanges();
                                                                        }
                                                                    }
                                                                }
                                                            }
                                                            bil = bil - Convert.ToDouble(rw["qty_demand"]);
                                                            rw["ord_assinged"] = Convert.ToDouble(rw["qty_demand"]);
                                                            rw.AcceptChanges();
                                                            tmp_ord.ImportRow(rw);
                                                        }
                                                    }
                                                    else
                                                    {
                                                        isfull = isfull + "'" + rek["indeks"].ToString() + "'_'" + rek["data_dost"].ToString() + "'";
                                                        rek["qty"] = bil;
                                                        rek.AcceptChanges();
                                                        break;
                                                    }
                                                }
                                                else
                                                {
                                                    Log("Calc_bal coś nie tak tego rekordu nie powinno być(ord_assing):" + rek["indeks"].ToString());
                                                }
                                            }
                                            else
                                            {
                                                iter = iter + 1;
                                            }
                                        }
                                        for (int i = 0; i < iter; i++)
                                        {
                                            mat_ord.Rows[i].Delete();
                                        }
                                        mat_ord.AcceptChanges();
                                        List<DataRow> row_delete = new List<DataRow>();
                                        if (sear_id != "")
                                        {
                                            foreach (DataRow rw in mat_ord.Rows)
                                            {
                                                if (sear_id.IndexOf("'" + rw["ordid"].ToString() + "'") > -1 || sear_id.IndexOf("'Zes" + rw["zest"].ToString() + "'") > -1)
                                                {
                                                    DataRow[] strf = ord_ids.Select("(zest_id='" + rw["ordid"].ToString() + "' or zest_id='Zes" + rw["zest"].ToString() + "') and ilo>0");
                                                    if (strf.Length > 0)
                                                    {
                                                        foreach (DataRow ra in strf)
                                                        {
                                                            if (Convert.ToDecimal(ra["ilo"]) == 1) { sear_id.Replace("'" + ra["zest_id"] + "'", ""); }
                                                            ra["ilo"] = Convert.ToDecimal(ra["ilo"]) - 1;
                                                            ra.AcceptChanges();
                                                        }
                                                    }
                                                    for (int k = j; k < mat_dmd.Rows.Count; k++)
                                                    {
                                                        DataRow rk = mat_dmd.Rows[k];
                                                        if (rk["indeks"].ToString() == rw["indeks"].ToString() && (DateTime)rk["data_dost"] == (DateTime)rw["data_dost"])
                                                        {
                                                            if (rw["ord_state"].ToString() != "Rozpoczęte")
                                                            {
                                                                if (isfull.IndexOf("'" + rw["indeks"].ToString() + "'_'" + rw["data_dost"].ToString() + "'") == -1)
                                                                {
                                                                    rk["qty"] = Convert.ToDouble(rk["qty"]) - Convert.ToDouble(rw["qty_demand"]);
                                                                    rw["ord_assinged"] = Convert.ToDouble(rw["qty_demand"]);
                                                                    tmp_ord.ImportRow(rw);
                                                                }
                                                            }
                                                            rk["il"] = (Int64)rk["il"] - 1;
                                                            row_delete.Add(rw);
                                                            if (Convert.ToDouble(rk["qty"]) - Convert.ToDouble(rw["qty_demand"]) <= Convert.ToDouble(rk["bil_chk"])) { isfull = isfull + "'" + rk["indeks"].ToString() + "'_'" + rk["data_dost"].ToString() + "'"; }
                                                            rw.AcceptChanges();
                                                            rk.AcceptChanges();
                                                            mat_dmd.AcceptChanges();
                                                        }
                                                    }
                                                }
                                                if (sear_id.Length < 3) { break; }
                                            }
                                            foreach (DataRow rw in row_delete)
                                            {
                                                mat_ord.Rows.Remove(rw);
                                            }
                                            mat_ord.AcceptChanges();
                                            int c = mat_ord.Rows.Count;
                                        }
                                    }
                                }
                                GC.Collect();

                                Log("End Calculate Data for Balanace for orders:" + (DateTime.Now - start));
                                using (DataTable sou_braki = new DataTable())
                                {
                                    using (NpgsqlConnection conA = new NpgsqlConnection(npA))
                                    {
                                        Log("Saving data to tmp table:" + (DateTime.Now - start));
                                        await conA.OpenAsync();

                                        using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                                            "select * " +
                                            "from public.braki " +
                                            "order by max_prod_date desc,indeks,data_dost,sort_ord desc", conA))
                                        {
                                            using (NpgsqlDataReader po = cmd.ExecuteReader())
                                            {
                                                using (DataTable sch = po.GetSchemaTable())
                                                {
                                                    foreach (DataRow a in sch.Rows)
                                                    {
                                                        sou_braki.Columns.Add(a["ColumnName"].ToString(), System.Type.GetType(a["DataType"].ToString()));
                                                        sou_braki.Columns[a["ColumnName"].ToString()].AllowDBNull = true;
                                                    }
                                                }
                                                sou_braki.Load(po);
                                            }
                                        }
                                        conA.Close();
                                    }
                                    DataRow[] new_rek = tmp_ord.Select("ord_assinged>0");
                                    if (new_rek.Length > 0)
                                    {
                                        using (NpgsqlConnection conA = new NpgsqlConnection(npA))
                                        {
                                            await conA.OpenAsync();
                                            using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                                                "UPDATE public.datatbles	" +
                                                "SET start_update=current_timestamp, in_progress=true " +
                                                "WHERE table_name='cal_ord1'", conA))
                                            {
                                                cmd.ExecuteNonQuery();
                                            }
                                            using (NpgsqlTransaction TR_BRAki = conA.BeginTransaction())
                                            {
                                                using (NpgsqlCommand cmd1 = new NpgsqlCommand("" +
                                                    "DELETE FROM public.braki_tmp; ", conA))
                                                {
                                                    cmd1.ExecuteNonQuery();
                                                }
                                                using (NpgsqlCommand cmd1 = new NpgsqlCommand("" +
                                                    "INSERT INTO public.braki_tmp " +
                                                    "(ordid, l_ordid, indeks, opis, planner_buyer, mag, data_dost, date_reuired, wlk_dost, bilans, typ_zdarzenia, status_informacji, dop, dop_lin, data_dop, zlec," +
                                                         " prod_date, max_posible_prod, max_prod_date, ord_supp_dmd, part_code, ord_state, prod_qty, qty_supply, qty_demand, koor, order_no, line_no, rel_no, part_no, descr, " +
                                                         "configuration, last_mail_conf, prom_date, prom_week, load_id, ship_date, state_conf, line_state, cust_ord_state, country, shipment_day, date_entered, sort_ord, zest, " +
                                                         "ord_assinged,id,cust_id) " +
                                                    "VALUES " +
                                                    "(@ordid, @l_ordid, @indeks, @opis, @planner_buyer, @mag, @data_dost, @date_reuired, @wlk_dost, @bilans, @typ_zdarzenia, @status_informacji, @dop, @dop_lin, @data_dop, @zlec, " +
                                                        "@prod_date, @max_posible_prod, @max_prod_date, @ord_supp_dmd, @part_code, @ord_state, @prod_qty, @qty_supply, @qty_demand, @koor, @order_no, @line_no, @rel_no, " +
                                                        "@part_no, @descr, @configuration, @last_mail_conf, @prom_date, @prom_week, @load_id, @ship_date, @state_conf, @line_state, @cust_ord_state, @country, @shipment_day, " +
                                                        "@date_entered, @sort_ord, @zest, @ord_assinged,@id,@custid); ", conA))
                                                {
                                                    cmd1.Parameters.Add("@ordid", NpgsqlTypes.NpgsqlDbType.Text);
                                                    cmd1.Parameters.Add("@l_ordid", NpgsqlTypes.NpgsqlDbType.Bigint);
                                                    cmd1.Parameters.Add("@indeks", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                    cmd1.Parameters.Add("@opis", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                    cmd1.Parameters.Add("@planner_buyer", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                    cmd1.Parameters.Add("@mag", NpgsqlTypes.NpgsqlDbType.Double);
                                                    cmd1.Parameters.Add("@data_dost", NpgsqlTypes.NpgsqlDbType.Date);
                                                    cmd1.Parameters.Add("@date_reuired", NpgsqlTypes.NpgsqlDbType.Date);
                                                    cmd1.Parameters.Add("@wlk_dost", NpgsqlTypes.NpgsqlDbType.Double);
                                                    cmd1.Parameters.Add("@bilans", NpgsqlTypes.NpgsqlDbType.Double);
                                                    cmd1.Parameters.Add("@typ_zdarzenia", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                    cmd1.Parameters.Add("@status_informacji", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                    cmd1.Parameters.Add("@dop", NpgsqlTypes.NpgsqlDbType.Integer);
                                                    cmd1.Parameters.Add("@dop_lin", NpgsqlTypes.NpgsqlDbType.Integer);
                                                    cmd1.Parameters.Add("@data_dop", NpgsqlTypes.NpgsqlDbType.Date);
                                                    cmd1.Parameters.Add("@zlec", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                    cmd1.Parameters.Add("@prod_date", NpgsqlTypes.NpgsqlDbType.Date);
                                                    cmd1.Parameters.Add("@max_posible_prod", NpgsqlTypes.NpgsqlDbType.Date);
                                                    cmd1.Parameters.Add("@max_prod_date", NpgsqlTypes.NpgsqlDbType.Date);
                                                    cmd1.Parameters.Add("@ord_supp_dmd", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                    cmd1.Parameters.Add("@part_code", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                    cmd1.Parameters.Add("@ord_state", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                    cmd1.Parameters.Add("@prod_qty", NpgsqlTypes.NpgsqlDbType.Double);
                                                    cmd1.Parameters.Add("@qty_supply", NpgsqlTypes.NpgsqlDbType.Double);
                                                    cmd1.Parameters.Add("@qty_demand", NpgsqlTypes.NpgsqlDbType.Double);
                                                    cmd1.Parameters.Add("@koor", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                    cmd1.Parameters.Add("@order_no", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                    cmd1.Parameters.Add("@line_no", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                    cmd1.Parameters.Add("@rel_no", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                    cmd1.Parameters.Add("@part_no", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                    cmd1.Parameters.Add("@descr", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                    cmd1.Parameters.Add("@configuration", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                    cmd1.Parameters.Add("@last_mail_conf", NpgsqlTypes.NpgsqlDbType.Date);
                                                    cmd1.Parameters.Add("@prom_date", NpgsqlTypes.NpgsqlDbType.Date);
                                                    cmd1.Parameters.Add("@prom_week", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                    cmd1.Parameters.Add("@load_id", NpgsqlTypes.NpgsqlDbType.Bigint);
                                                    cmd1.Parameters.Add("@ship_date", NpgsqlTypes.NpgsqlDbType.Date);
                                                    cmd1.Parameters.Add("@state_conf", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                    cmd1.Parameters.Add("@line_state", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                    cmd1.Parameters.Add("@cust_ord_state", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                    cmd1.Parameters.Add("@country", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                    cmd1.Parameters.Add("@shipment_day", NpgsqlTypes.NpgsqlDbType.Integer);
                                                    cmd1.Parameters.Add("@date_entered", NpgsqlTypes.NpgsqlDbType.Timestamp);
                                                    cmd1.Parameters.Add("@sort_ord", NpgsqlTypes.NpgsqlDbType.Timestamp);
                                                    cmd1.Parameters.Add("@zest", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                    cmd1.Parameters.Add("@ord_assinged", NpgsqlTypes.NpgsqlDbType.Double);
                                                    cmd1.Parameters.Add("@id", NpgsqlTypes.NpgsqlDbType.Uuid);
                                                    cmd1.Parameters.Add("@custid", NpgsqlTypes.NpgsqlDbType.Uuid);
                                                    cmd1.Prepare();
                                                    foreach (DataRow r in new_rek)
                                                    {
                                                        for (int i = 0; i < sou_braki.Columns.Count - 2; i++)
                                                        {
                                                            cmd1.Parameters[i].Value = r[i] ?? DBNull.Value;
                                                        }
                                                        cmd1.Parameters[sou_braki.Columns.Count - 2].Value = System.Guid.NewGuid();
                                                        cmd1.Parameters[sou_braki.Columns.Count - 1].Value = r[sou_braki.Columns.Count - 2] ?? DBNull.Value;
                                                        cmd1.ExecuteNonQuery();
                                                    }
                                                }
                                                using (NpgsqlCommand cmd1 = new NpgsqlCommand("" +
                                                    "UPDATE public.datatbles " +
                                                    "SET last_modify=current_timestamp, in_progress=false,updt_errors=false " +
                                                    "WHERE table_name='cal_ord1'", conA))
                                                {
                                                    cmd1.ExecuteNonQuery();
                                                }
                                                TR_BRAki.Commit();
                                            }
                                            conA.Close();
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                try
                {
                    using (NpgsqlConnection conA = new NpgsqlConnection(npA))
                    {
                        conA.Open();
                        using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                            "UPDATE public.datatbles	" +
                            "SET start_update=current_timestamp, in_progress=true " +
                            "WHERE table_name='cal_ord2'", conA))
                        {
                            cmd.ExecuteNonQuery();
                        }
                        using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                            "select cast(count(table_name) as integer) busy " +
                            "from public.datatbles " +
                            "where table_name='cal_ord1' and in_progress=true", conA))
                        {
                            int busy_il = 1;
                            while (busy_il > 0)
                            {
                                busy_il = Convert.ToInt16(cmd.ExecuteScalar());
                                if (busy_il > 0) { System.Threading.Thread.Sleep(250); }
                            }
                        }
                        using (NpgsqlTransaction TR_Braki = conA.BeginTransaction(IsolationLevel.ReadCommitted))
                        {
                            Log("Update main table step1:" + (DateTime.Now - start));
                            using (NpgsqlCommand cmd1 = new NpgsqlCommand("" +
                                "DELETE " +
                                "FROM public.braki " +
                                "WHERE id in " +
                                    "(select a.id from (select id,cust_id,ordid,dop_lin,zlec,data_dost,indeks from braki) a " +
                                    "left join " +
                                    "(select cust_id,ordid,dop_lin,zlec,data_dost,indeks from braki_tmp) b " +
                                    "on b.ordid=a.ordid and b.zlec=a.zlec and b.dop_lin=a.dop_lin and b.data_dost=a.data_dost and b.indeks=a.indeks " +
                                    "where b.ordid is null)", conA))
                            {
                                cmd1.ExecuteNonQuery();
                            }
                            Log("Update main table step2:" + (DateTime.Now - start));
                            using (NpgsqlCommand cmd1 = new NpgsqlCommand("" +
                                "UPDATE public.braki	" +
                                "SET l_ordid=a.l_ordid,opis=a.opis,planner_buyer=a.planner_buyer, mag=a.mag,date_reuired=a.date_reuired, wlk_dost=a.wlk_dost,bilans=a.bilans, " +
                                    "typ_zdarzenia=a.typ_zdarzenia, status_informacji=a.status_informacji, dop= a.dop,data_dop=a.data_dop, zlec=a.zlec, prod_date=a.prod_date,max_posible_prod=a.max_posible_prod, " +
                                    "max_prod_date=a.max_prod_date, ord_supp_dmd=a.ord_supp_dmd, part_code=a.part_code, ord_state=a.ord_state,prod_qty=a.prod_qty, qty_supply=a.qty_supply, qty_demand=a.qty_demand, " +
                                    "koor=a.koor, order_no=a.order_no, line_no=a.line_no, rel_no=a.rel_no, part_no=a.part_no, descr= a.descr,configuration=a.configuration, last_mail_conf=a.last_mail_conf, " +
                                    "prom_date=a.prom_date, prom_week=a.prom_week, load_id=a.load_id, ship_date=a.ship_date, state_conf=a.state_conf,line_state=a.line_state, cust_ord_state=a.cust_ord_state, " +
                                    "country=a.country, shipment_day=a.shipment_day, date_entered=a.date_entered, sort_ord=a.sort_ord,zest=a.zest,ord_assinged=a.ord_assinged,cust_id=a.cust_id " +
                                    "from " +
                                    "(select a.*,b.id " +
                                    "from" +
                                        "(select ordid, l_ordid, indeks, opis, planner_buyer, mag, data_dost, date_reuired, wlk_dost, bilans, typ_zdarzenia, status_informacji, dop, dop_lin, data_dop, zlec, " +
                                        "prod_date, max_posible_prod, max_prod_date, ord_supp_dmd, part_code, ord_state, prod_qty, qty_supply, qty_demand, koor, order_no, line_no, rel_no, part_no, descr, " +
                                        "configuration, last_mail_conf, prom_date, prom_week, load_id, ship_date, state_conf, line_state, cust_ord_state, country, shipment_day, date_entered, sort_ord, zest, " +
                                        "ord_assinged,cust_id from braki_tmp a  except  select ordid, l_ordid, indeks, opis, planner_buyer, mag, data_dost, date_reuired, wlk_dost, bilans, typ_zdarzenia, " +
                                        "status_informacji, dop, dop_lin, data_dop, zlec, prod_date, max_posible_prod, max_prod_date, ord_supp_dmd, part_code, ord_state, prod_qty, qty_supply, qty_demand, koor, " +
                                        "order_no, line_no, rel_no, part_no, descr, configuration, last_mail_conf, prom_date, prom_week, load_id, ship_date, state_conf, line_state, cust_ord_state, country, " +
                                        "shipment_day, date_entered, sort_ord, zest, ord_assinged,cust_id " +
                                        "from braki ) a " +
                                        "left join " +
                                        "(select cust_id,ordid,zlec,dop_lin,data_dost,indeks,id from braki) b " +
                                        "on b.ordid=a.ordid and b.zlec=a.zlec and b.dop_lin=a.dop_lin and b.data_dost=a.data_dost and b.indeks=a.indeks where b.ordid is not null) a " +
                                     "where public.braki.id=a.id", conA))
                            {
                                cmd1.ExecuteNonQuery();
                            }
                            Log("Update main table step3:" + (DateTime.Now - start));
                            using (NpgsqlCommand cmd1 = new NpgsqlCommand("" +
                                "insert into public.braki" +
                                    "(ordid, l_ordid, indeks, opis, planner_buyer, mag, data_dost, date_reuired, wlk_dost, bilans, typ_zdarzenia, status_informacji, dop, dop_lin, data_dop, zlec, " +
                                    "prod_date, max_posible_prod, max_prod_date, ord_supp_dmd, part_code, ord_state, prod_qty, qty_supply, qty_demand, koor, order_no, line_no, rel_no, part_no, descr, " +
                                    "configuration, last_mail_conf, prom_date, prom_week, load_id, ship_date, state_conf, line_state, cust_ord_state, country, shipment_day, date_entered, sort_ord, zest, " +
                                    "ord_assinged, id, cust_id) " +
                                        "select a.* " +
                                        "from " +
                                        "(select * from braki_tmp) a " +
                                        "left join " +
                                        "(select cust_id,ordid,dop_lin,zlec,data_dost,indeks from braki) b " +
                                        "on b.ordid=a.ordid and b.zlec=a.zlec and b.dop_lin=a.dop_lin and " +
                                        "b.data_dost=a.data_dost and b.indeks=a.indeks " +
                                     "where b.ordid is null", conA))
                            {
                                cmd1.ExecuteNonQuery();
                            }
                            using (NpgsqlCommand cmd1 = new NpgsqlCommand("" +
                                "UPDATE public.datatbles " +
                                "SET start_update=current_timestamp, in_progress=false,updt_errors=false " +
                                "WHERE substr(table_name,1,4)='mail'", conA))
                            {
                                cmd1.ExecuteNonQuery();
                            }
                            using (NpgsqlCommand cmd1 = new NpgsqlCommand("" +
                                "UPDATE public.datatbles " +
                                "SET last_modify=current_timestamp, in_progress=false,updt_errors=false " +
                                "WHERE table_name='cal_ord2'", conA))
                            {
                                cmd1.ExecuteNonQuery();
                            }
                            TR_Braki.Commit();
                        }
                        using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                            "UPDATE public.datatbles	" +
                            "SET start_update=current_timestamp, in_progress=true " +
                            "WHERE table_name='cal_ord3'", conA))
                        {
                            cmd.ExecuteNonQuery();
                        }
                        using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                            "UPDATE public.datatbles	" +
                            "SET start_update=current_timestamp, in_progress=true " +
                            "WHERE table_name='cal_ord5'", conA))
                        {
                            cmd.ExecuteNonQuery();
                        }
                        using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                            "select cast(count(table_name) as integer) busy " +
                            "from public.datatbles " +
                            "where table_name='cal_ord2' and in_progress=true", conA))
                        {
                            int busy_il = 1;
                            while (busy_il > 0)
                            {
                                busy_il = Convert.ToInt16(cmd.ExecuteScalar());
                                if (busy_il > 0) { System.Threading.Thread.Sleep(250); }
                            }
                        }
                        using (NpgsqlTransaction TR_refr = conA.BeginTransaction(IsolationLevel.ReadCommitted))
                        {
                            using (NpgsqlCommand cmd1 = new NpgsqlCommand("REFRESH MATERIALIZED VIEW to_mail", conA))
                            {
                                cmd1.ExecuteNonQuery();
                            }
                            using (NpgsqlCommand cmd1 = new NpgsqlCommand("" +
                                "UPDATE public.datatbles " +
                                "SET last_modify=current_timestamp, in_progress=false,updt_errors=false " +
                                "WHERE table_name='cal_ord5'", conA))
                            {
                                cmd1.ExecuteNonQuery();
                            }
                            TR_refr.Commit();
                        }
                        using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                            "select cast(count(table_name) as integer) busy " +
                            "from public.datatbles " +
                            "where table_name='cal_ord5' and in_progress=true", conA))
                        {
                            int busy_il = 1;
                            while (busy_il > 0)
                            {
                                busy_il = Convert.ToInt16(cmd.ExecuteScalar());
                                if (busy_il > 0) { System.Threading.Thread.Sleep(250); }
                            }
                        }
                        using (NpgsqlTransaction TR_MAILDEL = conA.BeginTransaction(IsolationLevel.ReadCommitted))
                        {
                            using (NpgsqlCommand cmd1 = new NpgsqlCommand("" +
                                "delete " +
                                "from mail " +
                                "where status_informacji='NOT IMPLEMENT' or cust_line_stat!='Aktywowana'", conA))
                            {
                                cmd1.ExecuteNonQuery();
                            }
                            Log("Update mail step1:" + (DateTime.Now - start));
                            using (NpgsqlCommand cmd1 = new NpgsqlCommand("" +
                                "DELETE FROM public.mail " +
                                "WHERE cust_id not in (select id from public.cust_ord)", conA))
                            {
                                cmd1.ExecuteNonQuery();
                            }
                            Log("Update mail step:" + (DateTime.Now - start));
                            // using (NpgsqlCommand cmd1 = new NpgsqlCommand("DELETE FROM public.mail WHERE cust_id in (select a.cust_id from mail a left join braki c on c.cust_id=a.cust_id or c.zest=a.zest,cust_ord b where a.indeks!='ZESTAW' and a.cust_id=b.id and b.data_dop<a.prod and c.cust_id is null)", conA))
                            using (NpgsqlCommand cmd1 = new NpgsqlCommand("" +
                                "DELETE FROM public.mail	" +
                                "WHERE cust_id in (select a.cust_id " +
                                    "from mail a " +
                                    "left join " +
                                    "to_mail b " +
                                    "on b.cust_id=a.cust_id " +
                                    "where b.cust_id is null and (is_for_mail(a.status_informacji)=false or a.status_informacji='POPRAWIĆ'))", conA))
                            {
                                cmd1.ExecuteNonQuery();
                            }
                            using (NpgsqlCommand cmd1 = new NpgsqlCommand("" +
                                "UPDATE public.datatbles " +
                                "SET last_modify=current_timestamp, in_progress=false,updt_errors=false " +
                                "WHERE table_name='cal_ord3'", conA))
                            {
                                cmd1.ExecuteNonQuery();
                            }
                            TR_MAILDEL.Commit();
                        }
                        using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                            "UPDATE public.datatbles	" +
                            "SET start_update=current_timestamp, in_progress=true " +
                            "WHERE table_name='cal_ord4'", conA))
                        {
                            cmd.ExecuteNonQuery();
                        }
                        using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                            "select cast(count(table_name) as integer) busy " +
                            "from public.datatbles " +
                            "where table_name='cal_ord3' and in_progress=true", conA))
                        {
                            int busy_il = 1;
                            while (busy_il > 0)
                            {
                                busy_il = Convert.ToInt16(cmd.ExecuteScalar());
                                if (busy_il > 0) { System.Threading.Thread.Sleep(250); }
                            }
                        }
                        using (NpgsqlTransaction TR_MAIL = conA.BeginTransaction(IsolationLevel.ReadCommitted))
                        {
                            Log("Update mail step0:" + (DateTime.Now - start));
                            using (NpgsqlCommand cmd1 = new NpgsqlCommand("" +
                                "INSERT INTO public.mail" +
                                    "(ordid,dop, koor, order_no, line_no, rel_no, part_no, descr, country, prom_date, prom_week, load_id, ship_date, date_entered, cust_id, prod, prod_week, planner_buyer, indeks, " +
                                    "opis, typ_zdarzenia, status_informacji, zest, info_handlo, logistyka, seria0, data0, cust_line_stat, ord_objver) " +
                                 "select * from " +
                                    "(select a.* " +
                                    "from to_mail a " +
                                    "left join (select cust_id from mail) b " +
                                    "on b.cust_id=a.cust_id " +
                                    "where a.cust_id is not null and b.cust_id is null  " +
                                    "order by order_no,line_no,rel_no) a  " +
                                 "group by a.ordid,a.dop,a.koor,a.order_no,a.line_no,a.rel_no,a.part_no,a.descr,a.country,a.prom_date,a.prom_week,a.load_id,a.ship_date,a.date_entered,a.cust_id," +
                                 "a.prod,a.prod_week,a.planner_buyer,a.indeks,a.opis,a.typ_zdarzenia,a.status_informacji,a.zest,a.info_handlo,a.logistyka,a.seria0,a.data0,a.cust_line_stat,a.ord_objver", conA))
                            {
                                cmd1.ExecuteNonQuery();
                            }
                            Log("Update mail step2:" + (DateTime.Now - start));
                            using (NpgsqlCommand cmd1 = new NpgsqlCommand("" +
                                "UPDATE public.mail a " +
                                "SET ordid=b.ordid,dop=b.dop,koor=b.koor,order_no=b.order_no,line_no=b.line_no,rel_no=b.rel_no,part_no=b.part_no,descr=b.descr,country=b.country,prom_date=b.prom_date," +
                                "prom_week=b.prom_week,load_id=b.load_id,ship_date=b.ship_date,date_entered=b.date_entered,prod=b.prod,prod_week=b.prod_week,planner_buyer=b.planner_buyer,indeks=b.indeks,opis=b.opis," +
                                "typ_zdarzenia=b.typ_zdarzenia,status_informacji=b.status_informacji,zest=b.zest,info_handlo=b.info_handlo,logistyka=b.logistyka,seria0=b.seria0,data0=b.data0,cust_line_stat=b.cust_line_stat " +
                                "from " +
                                    "(select * from " +
                                        "(select a.ordID,a.dop,a.koor,a.order_no,a.line_no,a.rel_no,a.part_no,a.descr,a.country,a.prom_date,a.prom_week,a.load_id,a.ship_date,a.date_entered,a.cust_id,a.prod," +
                                        "a.prod_week,a.planner_buyer,a.indeks,a.opis,a.typ_zdarzenia,a.status_informacji,a.zest,a.info_handlo,a.logistyka,a.seria0,a.data0,a.cust_line_stat " +
                                        "from " +
                                        "to_mail a, " +
                                        "mail b " +
                                        "where b.cust_id=a.cust_id " +
                                        "except " +
                                        "select ordID,dop,koor,order_no,line_no,rel_no,part_no,descr,country,prom_date,prom_week,load_id,ship_date,date_entered,cust_id,prod,prod_week,planner_buyer,indeks,opis," +
                                        "typ_zdarzenia,status_informacji,zest,info_handlo,logistyka,seria0,data0,cust_line_stat from mail ) a) b  " +
                                     "where b.cust_id=a.cust_id;", conA))
                            {
                                cmd1.ExecuteNonQuery();
                            }
                            // usuń z mail już przesunięte zamówienia dla których nie było żadnych zagrożeń 
                            Log("Update mail step3:" + (DateTime.Now - start));

                            using (NpgsqlCommand cmd1 = new NpgsqlCommand("" +
                                "update public.mail a " +
                                "SET ordid=b.ordid,dop=b.dop,koor=b.koor,order_no=b.order_no,line_no=b.line_no,rel_no=b.rel_no,part_no=b.part_no,descr=b.descr,country=b.country,prom_date=b.prom_date," +
                                "prom_week=b.prom_week,load_id=b.load_id,ship_date=b.ship_date,date_entered=b.date_entered,prod=b.prod,prod_week=b.prod_week,info_handlo=b.info_handlo,logistyka=b.logistyka," +
                                "seria0=b.seria0,data0=b.data0,cust_line_stat=b.cust_line_stat " +
                                "from " +
                                    "(select a.* " +
                                        "from (Select case when c.dop_connection_db!='AUT' then 'O '||c.order_no else 'D'||to_char(c.dop_id,'9999999999') end ordID,c.dop_id as dop,c.koor,c.order_no,c.line_no," +
                                        "c.rel_no,c.part_no,c.descr,c.country,c.prom_date,cast(c.prom_week as integer) as prom_week,c.load_id,c.ship_date, c.date_entered,c.id as cust_id,c.data_dop as prod," +
                                        "case when to_date(to_char(c.data_dop,'iyyyiw'),'iyyyiw') + shipment_day(c.country,c.cust_no,c.zip_code,c.addr1)-1>c.data_dop then " +
                                            "cast(to_char(c.data_dop,'iyyyiw') as integer) else cast(to_char(c.data_dop + interval '6 day','iyyyiw') as integer) end as prod_week," +
                                        "c.planner_buyer,c.indeks,c.opis,c.status_informacji,c.zest," +
                                        "case when case when to_date(to_char(c.data_dop,'iyyyiw'),'iyyyiw') + shipment_day(c.country,c.cust_no,c.zip_code,c.addr1)-1>c.data_dop then " +
                                            "cast(to_char(c.data_dop,'iyyyiw') as integer) else cast(to_char(c.data_dop + interval '6 day','iyyyiw') as integer) end>cast(c.prom_week as integer) then " +
                                                "true else false end as info_handlo," +
                                         "case when c.ship_date is null then false else case when c.data_dop<c.ship_date then " +
                                            "false else true  end end as logistyka,c.seria0,c.data0,c.line_state as cust_line_stat " +
                                         "from (select c.*,a.planner_buyer,a.indeks,a.opis,a.status_informacji " +
                                            "from " +
                                            "mail a " +
                                            "left join " +
                                            "to_mail b " +
                                            "on b.cust_id=a.cust_id " +
                                            "left join " +
                                            "cust_ord c " +
                                            "on c.id=a.cust_id " +
                                            "where b.cust_id is null )c " +
                                            "except " +
                                            "select ordID,dop,koor,order_no,line_no,rel_no,part_no,descr,country,prom_date,prom_week,load_id,ship_date,date_entered,cust_id,prod,prod_week,planner_buyer,indeks,opis," +
                                            "status_informacji,zest,info_handlo,logistyka,seria0,data0,cust_line_stat  from mail ) a) b " +
                                      "where a.cust_id=b.cust_id", conA))
                            {
                                cmd1.ExecuteNonQuery();
                            }
                            using (NpgsqlCommand cmd1 = new NpgsqlCommand("" +
                                "UPDATE public.datatbles " +
                                "SET last_modify=current_timestamp, in_progress=false,updt_errors=false " +
                                "WHERE substr(table_name,1,4)='mail'", conA))
                            {
                                cmd1.ExecuteNonQuery();
                            }
                            using (NpgsqlCommand cmd1 = new NpgsqlCommand("" +
                                "DELETE FROM public.mail	" +
                                "WHERE cust_id in (select a.cust_id " +
                                    "from mail a " +
                                    "left join " +
                                    "to_mail b " +
                                    "on b.cust_id=a.cust_id " +
                                    "left join " +
                                    "cust_ord c " +
                                    "on c.id=a.cust_id " +
                                    "where b.cust_id is null and is_for_mail(a.status_informacji)=true and c.data_dop>=a.prod " +
                                    "and cast(c.prom_week as integer)>=a.prod_week and (c.ship_date is null or c.ship_date>a.prod))", conA))
                            {
                                cmd1.ExecuteNonQuery();
                            }
                            using (NpgsqlCommand cmd1 = new NpgsqlCommand("" +
                                "delete " +
                                "from braki_hist " +
                                "where objversion<current_timestamp - interval '7 day'", conA))
                            {
                                cmd1.ExecuteNonQuery();
                            }
                            using (NpgsqlCommand cmd1 = new NpgsqlCommand("" +
                                "delete " +
                                "from mail_hist " +
                                "where date_addd<current_timestamp - interval '7 day'", conA))
                            {
                                cmd1.ExecuteNonQuery();
                            }
                            using (NpgsqlCommand cmd1 = new NpgsqlCommand("" +
                                "UPDATE public.datatbles " +
                                "SET last_modify=current_timestamp, in_progress=false,updt_errors=false " +
                                "WHERE table_name='cal_ord4'", conA))
                            {
                                cmd1.ExecuteNonQuery();
                            }
                            TR_MAIL.Commit();
                        }
                    }
                    GC.Collect();
                    using (NpgsqlConnection conA = new NpgsqlConnection(npC))
                    {
                        await conA.OpenAsync();
                        using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                            "UPDATE public.datatbles " +
                            "SET last_modify=current_timestamp, in_progress=false,updt_errors=false " +
                            "WHERE substr(table_name,1,5)='braki'", conA))
                        {
                            cmd.ExecuteNonQuery();
                        }
                        conA.Close();
                    }
                    int rwa = await Modify_prod_date();
                    return 0;
                }
                catch (Exception e)
                {
                    Log("Błąd Problem z aktualizacją braków:" + e);
                    using (NpgsqlConnection conA = new NpgsqlConnection(npA))
                    {
                        await conA.OpenAsync();
                        using (NpgsqlCommand cmd1 = new NpgsqlCommand("" +
                            "delete " +
                            "from braki_hist" +
                            " where id in (select b.id from braki a,braki_hist b where a.id=b.id)", conA))
                        {
                            cmd1.ExecuteNonQuery();
                        }
                        conA.Close();
                    }
                    return 1;
                }
            }
            catch (Exception e)
            {
                using (NpgsqlConnection conA = new NpgsqlConnection(npC))
                {
                    await conA.OpenAsync();
                    using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                        "UPDATE public.datatbles " +
                        "SET in_progress=false,updt_errors=true " +
                        "WHERE substr(table_name,1,5)='braki'", conA))
                    {
                        cmd.ExecuteNonQuery();
                    }
                    conA.Close();
                }
                Log("Problem z obliczaniem bilansów braków:" + e);
                return 1;
            }
        }
        private async Task<int> Lack_bil()
        {
            Log("Start BALANCE LACK");
            try
            {
                using (NpgsqlConnection conA = new NpgsqlConnection(npA))
                {
                    conA.Open();
                    using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                        "select cast(count(table_name) as integer) busy " +
                        "from public.datatbles " +
                        "where (table_name='demands' or substring(table_name,1,6)='worker') and in_progress=true", conA))
                    {
                        int busy_il = 1;
                        while (busy_il > 0)
                        {
                            busy_il = Convert.ToInt16(cmd.ExecuteScalar());
                            if (busy_il > 0) { System.Threading.Thread.Sleep(250); }
                        }
                    }
                }
                using (DataTable aggr = new DataTable())
                {
                    using (NpgsqlConnection conA = new NpgsqlConnection(npA))
                    {
                        conA.Open();
                        using (NpgsqlCommand cmd1 = new NpgsqlCommand("" +
                            "UPDATE public.datatbles " +
                            "SET start_update=current_timestamp, in_progress=true,updt_errors=false " +
                            "WHERE table_name='ord_l'", conA))
                        {
                            cmd1.ExecuteNonQuery();
                        }
                        using (NpgsqlTransaction TR_updtlack = conA.BeginTransaction(IsolationLevel.ReadCommitted))
                        {
                            using (NpgsqlCommand cmd1 = new NpgsqlCommand("" +
                                "UPDATE public.datatbles " +
                                "SET start_update=current_timestamp, in_progress=true,updt_errors=false " +
                                "WHERE table_name='ord_lack_bil'", conA))
                            {
                                cmd1.ExecuteNonQuery();
                            }
                            using (NpgsqlCommand cmd = new NpgsqlCommand("DELETE FROM public.ord_lack_bil", conA))
                            {
                                cmd.ExecuteNonQuery();
                            }
                            using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                                "INSERT INTO public.ord_lack_bil" +
                                "(dop, dop_lin, data_dop, day_shift, order_no, line_no, rel_no, int_ord, contract, order_supp_dmd, wrkc, next_wrkc, part_no, descr, part_code, date_required, ord_state, ord_date, " +
                                "prod_qty, qty_supply, qty_demand, dat_creat, chksum, id) " +
                                "select b.* " +
                                "from " +
                                    "(select a.part_no,a.work_day,b.min_lack,a.balance *-1 lack,case when a.work_day=b.min_lack then case when a.balance*-1=a.qty_demand then 'All' else 'Part' end else 'All' end stat_lack " +
                                    "from " +
                                        "(select part_no,work_day,qty_demand,balance from demands where balance<0 and qty_demand!=0 and koor!='LUCPRZ') a " +
                                        "left join " +
                                        "(select part_no,min(work_day) min_lack from demands where balance<0 and qty_demand!=0 group by part_no) b " +
                                        "on b.part_no=a.part_no " +
                                        "where case when a.work_day=b.min_lack then case when a.balance*-1=a.qty_demand then 'All' else 'Part' end else 'All' end='All') a," +
                                 "ord_demands b " +
                                 "where b.part_no=a.part_no and b.date_required=a.work_day", conA))
                            {
                                cmd.ExecuteNonQuery();
                            }
                            using (NpgsqlCommand cmd1 = new NpgsqlCommand("" +
                                "UPDATE public.datatbles " +
                                "SET last_modify=current_timestamp, in_progress=false,updt_errors=false " +
                                "WHERE table_name='ord_l'", conA))
                            {
                                cmd1.ExecuteNonQuery();
                            }
                            TR_updtlack.Commit();
                        }
                    }
                    using (NpgsqlConnection conA = new NpgsqlConnection(npC))
                    {
                        conA.Open();
                        using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                            "select cast(count(table_name) as integer) busy " +
                            "from public.datatbles " +
                            "where table_name='ord_l' and in_progress=true", conA))
                        {
                            int busy_il = 1;
                            while (busy_il > 0)
                            {
                                busy_il = Convert.ToInt16(cmd.ExecuteScalar());
                                if (busy_il > 0) { System.Threading.Thread.Sleep(250); }
                            }
                        }
                    }
                    using (NpgsqlConnection conA = new NpgsqlConnection(npA))
                    {
                        await conA.OpenAsync();
                        using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                            "select b.*,a.lack bal_stock " +
                            "from " +
                                "(select a.part_no,a.work_day,b.min_lack,a.balance *-1 lack," +
                                "case when a.work_day=b.min_lack then " +
                                    "case when a.balance*-1=a.qty_demand then 'All' else 'Part' end else 'All' end stat_lack " +
                                "from " +
                                    "(select part_no,work_day,qty_demand,balance " +
                                    "from demands where balance<0 and qty_demand!=0 and koor!='LUCPRZ') a " +
                                    "left join " +
                                    "(select part_no,min(work_day) min_lack " +
                                    "from demands " +
                                    "where balance<0 and qty_demand!=0 group by part_no) b " +
                                    "on b.part_no=a.part_no " +
                                "where case when a.work_day=b.min_lack then case when a.balance*-1=a.qty_demand then 'All' else 'Part' end else 'All' end='Part') a," +
                                "ord_demands b " +
                            "where b.part_no=a.part_no and b.date_required=a.work_day " +
                            "order by part_no,date_required,int_ord desc", conA))
                        {
                            using (NpgsqlDataReader po = cmd.ExecuteReader())
                            {
                                using (DataTable sch = po.GetSchemaTable())
                                {
                                    foreach (DataRow a in sch.Rows)
                                    {
                                        aggr.Columns.Add(a["ColumnName"].ToString(), System.Type.GetType(a["DataType"].ToString()));
                                        aggr.Columns[a["ColumnName"].ToString()].AllowDBNull = true;
                                    }
                                    aggr.Columns.Add("Stat");
                                    aggr.Columns["Stat"].AllowDBNull = true;
                                }
                                aggr.Load(po);
                                Log("Data for Balanace for orders_bil:" + (DateTime.Now - start));
                            }
                        }
                        conA.Close();
                    }
                    string Part_no = "";
                    DateTime nullDAT = start.AddDays(1000);
                    DateTime part_dat = nullDAT;
                    double bil = 0;
                    foreach (DataRowView rek in aggr.DefaultView)
                    {
                        if (Part_no != rek["Part_no"].ToString() || part_dat != (DateTime)rek["date_required"])
                        {
                            Part_no = rek["Part_no"].ToString();
                            part_dat = (DateTime)rek["date_required"];
                            bil = Convert.ToDouble(rek["bal_stock"]);
                        }
                        if (bil > 0)
                        {
                            rek["Stat"] = "ADD";
                            bil = bil - Convert.ToDouble(rek["qty_demand"]);
                        }
                    }
                    DataRow[] rwA = aggr.Select("Stat='ADD'");
                    Log("RECORDS LACK_bil add: " + rwA.Length);
                    if (rwA.Length > 0)
                    {
                        using (NpgsqlConnection conB = new NpgsqlConnection(npA))
                        {
                            await conB.OpenAsync();
                            using (NpgsqlTransaction TR_ins_ord_lack = conB.BeginTransaction(IsolationLevel.ReadCommitted))
                            {
                                using (NpgsqlCommand cmd2 = new NpgsqlCommand("" +
                                    "INSERT INTO public.ord_lack_bil" +
                                    "(dop, dop_lin, data_dop, day_shift, order_no, line_no, rel_no, int_ord ,contract, order_supp_dmd, wrkc, next_wrkc, part_no, descr, part_code, date_required, ord_state, " +
                                    "ord_date, prod_qty, qty_supply, qty_demand,dat_creat , chksum, id) " +
                                    "VALUES " +
                                    "(@dop,@dop_lin,@data_dop,@day_shift,@order_no,@line_no,@rel_no,@int_ord,@contract,@order_supp_dmd,@wrkc,@next_wrkc,@part_no,@descr,@part_code,@date_required," +
                                    "@ord_state,@ord_date,@prod_qty,@qty_supply,@qty_demand,@dat_creat,@chksum,@id);", conB))
                                {
                                    cmd2.Parameters.Add("dop", NpgsqlTypes.NpgsqlDbType.Integer);
                                    cmd2.Parameters.Add("dop_lin", NpgsqlTypes.NpgsqlDbType.Integer);
                                    cmd2.Parameters.Add("data_dop", NpgsqlTypes.NpgsqlDbType.Date);
                                    cmd2.Parameters.Add("day_shift", NpgsqlTypes.NpgsqlDbType.Integer);
                                    cmd2.Parameters.Add("order_no", NpgsqlTypes.NpgsqlDbType.Varchar);
                                    cmd2.Parameters.Add("line_no", NpgsqlTypes.NpgsqlDbType.Varchar);
                                    cmd2.Parameters.Add("rel_no", NpgsqlTypes.NpgsqlDbType.Varchar);
                                    cmd2.Parameters.Add("int_ord", NpgsqlTypes.NpgsqlDbType.Integer);
                                    cmd2.Parameters.Add("contract", NpgsqlTypes.NpgsqlDbType.Varchar);
                                    cmd2.Parameters.Add("order_supp_dmd", NpgsqlTypes.NpgsqlDbType.Varchar);
                                    cmd2.Parameters.Add("wrkc", NpgsqlTypes.NpgsqlDbType.Varchar);
                                    cmd2.Parameters.Add("next_wrkc", NpgsqlTypes.NpgsqlDbType.Varchar);
                                    cmd2.Parameters.Add("part_no", NpgsqlTypes.NpgsqlDbType.Varchar);
                                    cmd2.Parameters.Add("descr", NpgsqlTypes.NpgsqlDbType.Varchar);
                                    cmd2.Parameters.Add("part_code", NpgsqlTypes.NpgsqlDbType.Varchar);
                                    cmd2.Parameters.Add("date_required", NpgsqlTypes.NpgsqlDbType.Date);
                                    cmd2.Parameters.Add("ord_state", NpgsqlTypes.NpgsqlDbType.Varchar);
                                    cmd2.Parameters.Add("ord_date", NpgsqlTypes.NpgsqlDbType.Date);
                                    cmd2.Parameters.Add("prod_qty", NpgsqlTypes.NpgsqlDbType.Double);
                                    cmd2.Parameters.Add("qty_supply", NpgsqlTypes.NpgsqlDbType.Double);
                                    cmd2.Parameters.Add("qty_demand", NpgsqlTypes.NpgsqlDbType.Double);
                                    cmd2.Parameters.Add("dat_creat", NpgsqlTypes.NpgsqlDbType.Date);
                                    cmd2.Parameters.Add("chksum", NpgsqlTypes.NpgsqlDbType.Integer);
                                    cmd2.Parameters.Add("id", NpgsqlTypes.NpgsqlDbType.Uuid);
                                    cmd2.Prepare();
                                    foreach (DataRow rA in rwA)
                                    {
                                        for (int i = 0; i < 24; i++)
                                        {
                                            cmd2.Parameters[i].Value = rA[i];
                                        }
                                        cmd2.ExecuteNonQuery();
                                    }
                                    Log("END ADD LACK_bil:" + (DateTime.Now - start));
                                }
                                using (NpgsqlCommand cmd1 = new NpgsqlCommand("" +
                                    "UPDATE public.datatbles " +
                                    "SET last_modify=current_timestamp, in_progress=false,updt_errors=false " +
                                    "WHERE table_name='ord_lack_bil'", conB))
                                {
                                    cmd1.ExecuteNonQuery();
                                }
                                TR_ins_ord_lack.Commit();
                            }
                            conB.Close();
                        }
                    }
                    using (NpgsqlConnection conA = new NpgsqlConnection(npA))
                    {
                        await conA.OpenAsync();
                        using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                            "select cast(count(table_name) as integer) busy " +
                            "from public.datatbles " +
                            "where table_name='ord_lack_bil' and in_progress=true", conA))
                        {
                            int busy_il = 1;
                            while (busy_il > 0)
                            {
                                busy_il = Convert.ToInt16(cmd.ExecuteScalar());
                                if (busy_il > 0) { System.Threading.Thread.Sleep(250); }
                            }
                        }
                        using (NpgsqlCommand cmd = new NpgsqlCommand("REFRESH MATERIALIZED VIEW formatka_bil; ", conA))
                        {
                            cmd.ExecuteNonQuery();
                        }
                        conA.Close();
                    }
                }
                return 0;
            }
            catch (Exception e)
            {
                Log("ERORR in LACK_bil RPT" + e);
                return 1;
            }
        }
        private async Task<int> All_lacks()
        {
            Log("Start LACK");
            try
            {
                using (NpgsqlConnection conB = new NpgsqlConnection(npA))
                {
                    conB.Open();
                    using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                        "select cast(count(table_name) as integer) busy " +
                        "from public.datatbles " +
                        "where (table_name='demands' or substring(table_name,1,6)='worker') and in_progress=true", conB))
                    {
                        int busy_il = 1;
                        while (busy_il > 0)
                        {
                            busy_il = Convert.ToInt16(cmd.ExecuteScalar());
                            if (busy_il > 0) { System.Threading.Thread.Sleep(250); }
                        }
                    }
                }
                using (DataTable aggr = new DataTable())
                {
                    using (NpgsqlConnection conA = new NpgsqlConnection(npA))
                    {
                        conA.Open();
                        using (NpgsqlCommand cmd1 = new NpgsqlCommand("" +
                            "UPDATE public.datatbles " +
                            "SET start_update=current_timestamp, in_progress=true,updt_errors=false " +
                            "WHERE table_name='ord_lack'", conA))
                        {
                            cmd1.ExecuteNonQuery();
                        }
                        using (NpgsqlTransaction TR_insord_lack = conA.BeginTransaction())
                        {
                            using (NpgsqlCommand cmd = new NpgsqlCommand("DELETE FROM public.ord_lack", conA))
                            {
                                cmd.ExecuteNonQuery();
                            }
                            using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                                "INSERT INTO public.ord_lack" +
                                "(dop, dop_lin, data_dop, day_shift, order_no, line_no, rel_no, int_ord, contract, order_supp_dmd, wrkc, next_wrkc, part_no, descr, part_code, date_required, ord_state, ord_date, " +
                                "prod_qty, qty_supply, qty_demand, dat_creat, chksum, id) " +
                                "select b.* " +
                                "from " +
                                    "(select a.part_no,a.work_day,b.min_lack,a.bal_stock *-1 lack, case when a.work_day=b.min_lack then case when a.bal_stock*-1=a.qty_demand then 'All' else 'Part' end else 'All' end stat_lack " +
                                    "from " +
                                        "(select part_no,work_day,qty_demand,bal_stock from " +
                                        "demands " +
                                        "where bal_stock<0 and qty_demand!=0 and work_day<date_fromnow(10) and koor!='LUCPRZ') a " +
                                        "left join " +
                                        "(select part_no,min(work_day) min_lack from " +
                                        "demands where bal_stock<0 and qty_demand!=0 and work_day<date_fromnow(10) group by part_no) b " +
                                        "on b.part_no=a.part_no " +
                                        "where case when a.work_day=b.min_lack then case when a.bal_stock*-1=a.qty_demand then 'All' else 'Part' end else 'All' end='All') a," +
                                   "ord_demands b " +
                                   "where b.part_no=a.part_no and b.date_required=a.work_day", conA))
                            {
                                cmd.ExecuteNonQuery();
                            }
                            TR_insord_lack.Commit();
                        }
                    }
                    using (NpgsqlConnection conA = new NpgsqlConnection(npA))
                    {
                        await conA.OpenAsync();
                        using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                            "select b.*,a.lack bal_stock " +
                            "from " +
                            "(select a.part_no,a.work_day,b.min_lack,a.bal_stock *-1 lack, case when a.work_day=b.min_lack then case when a.bal_stock*-1=a.qty_demand then 'All' else 'Part' end else 'All' end stat_lack " +
                            "from " +
                                "(select part_no,work_day,qty_demand,bal_stock " +
                                "from demands " +
                                "where bal_stock<0 and qty_demand!=0 and work_day<date_fromnow(10) and koor!='LUCPRZ') a " +
                                "left join " +
                                "(select part_no,min(work_day) min_lack " +
                                "from demands " +
                                "where bal_stock<0 and qty_demand!=0 and work_day<date_fromnow(10) group by part_no) b " +
                                "on b.part_no=a.part_no " +
                                "where case when a.work_day=b.min_lack then case when a.bal_stock*-1=a.qty_demand then 'All' else 'Part' end else 'All' end='Part') a," +
                             "ord_demands b where b.part_no=a.part_no and b.date_required=a.work_day " +
                             "order by part_no,date_required,int_ord desc ", conA))
                        {
                            using (NpgsqlDataReader po = cmd.ExecuteReader())
                            {
                                using (DataTable sch = po.GetSchemaTable())
                                {
                                    foreach (DataRow a in sch.Rows)
                                    {
                                        aggr.Columns.Add(a["ColumnName"].ToString(), System.Type.GetType(a["DataType"].ToString()));
                                        aggr.Columns[a["ColumnName"].ToString()].AllowDBNull = true;
                                    }
                                    aggr.Columns.Add("Stat");
                                    aggr.Columns["Stat"].AllowDBNull = true;
                                }
                                aggr.Load(po);
                                Log("Data for Balanace for orders:" + (DateTime.Now - start));
                            }
                        }
                        conA.Close();
                    }
                    string Part_no = "";
                    DateTime nullDAT = start.AddDays(1000);
                    DateTime part_dat = nullDAT;
                    double bil = 0;
                    foreach (DataRowView rek in aggr.DefaultView)
                    {
                        if (Part_no != rek["Part_no"].ToString() || part_dat != (DateTime)rek["date_required"])
                        {
                            Part_no = rek["Part_no"].ToString();
                            part_dat = (DateTime)rek["date_required"];
                            bil = Convert.ToDouble(rek["bal_stock"]);
                        }
                        if (bil > 0)
                        {
                            rek["Stat"] = "ADD";
                            bil = bil - Convert.ToDouble(rek["qty_demand"]);
                        }
                    }
                    DataRow[] rwA = aggr.Select("Stat='ADD'");
                    Log("RECORDS LACK add: " + rwA.Length);
                    if (rwA.Length > 0)
                    {
                        using (NpgsqlConnection conB = new NpgsqlConnection(npA))
                        {
                            conB.Open();
                            using (NpgsqlTransaction TR_inso_lack = conB.BeginTransaction())
                            {
                                using (NpgsqlCommand cmd2 = new NpgsqlCommand("" +
                                    "INSERT INTO public.ord_lack" +
                                    "(dop, dop_lin, data_dop, day_shift, order_no, line_no, rel_no, int_ord ,contract, order_supp_dmd, wrkc, next_wrkc, part_no, descr, part_code, date_required, ord_state, " +
                                    "ord_date, prod_qty, qty_supply, qty_demand,dat_creat , chksum, id) " +
                                    "VALUES " +
                                    "(@dop,@dop_lin,@data_dop,@day_shift,@order_no,@line_no,@rel_no,@int_ord,@contract,@order_supp_dmd,@wrkc,@next_wrkc,@part_no,@descr,@part_code,@date_required,@ord_state,@ord_date," +
                                    "@prod_qty,@qty_supply,@qty_demand,@dat_creat,@chksum,@id);", conB))
                                {
                                    cmd2.Parameters.Add("dop", NpgsqlTypes.NpgsqlDbType.Integer);
                                    cmd2.Parameters.Add("dop_lin", NpgsqlTypes.NpgsqlDbType.Integer);
                                    cmd2.Parameters.Add("data_dop", NpgsqlTypes.NpgsqlDbType.Date);
                                    cmd2.Parameters.Add("day_shift", NpgsqlTypes.NpgsqlDbType.Integer);
                                    cmd2.Parameters.Add("order_no", NpgsqlTypes.NpgsqlDbType.Varchar);
                                    cmd2.Parameters.Add("line_no", NpgsqlTypes.NpgsqlDbType.Varchar);
                                    cmd2.Parameters.Add("rel_no", NpgsqlTypes.NpgsqlDbType.Varchar);
                                    cmd2.Parameters.Add("int_ord", NpgsqlTypes.NpgsqlDbType.Integer);
                                    cmd2.Parameters.Add("contract", NpgsqlTypes.NpgsqlDbType.Varchar);
                                    cmd2.Parameters.Add("order_supp_dmd", NpgsqlTypes.NpgsqlDbType.Varchar);
                                    cmd2.Parameters.Add("wrkc", NpgsqlTypes.NpgsqlDbType.Varchar);
                                    cmd2.Parameters.Add("next_wrkc", NpgsqlTypes.NpgsqlDbType.Varchar);
                                    cmd2.Parameters.Add("part_no", NpgsqlTypes.NpgsqlDbType.Varchar);
                                    cmd2.Parameters.Add("descr", NpgsqlTypes.NpgsqlDbType.Varchar);
                                    cmd2.Parameters.Add("part_code", NpgsqlTypes.NpgsqlDbType.Varchar);
                                    cmd2.Parameters.Add("date_required", NpgsqlTypes.NpgsqlDbType.Date);
                                    cmd2.Parameters.Add("ord_state", NpgsqlTypes.NpgsqlDbType.Varchar);
                                    cmd2.Parameters.Add("ord_date", NpgsqlTypes.NpgsqlDbType.Date);
                                    cmd2.Parameters.Add("prod_qty", NpgsqlTypes.NpgsqlDbType.Double);
                                    cmd2.Parameters.Add("qty_supply", NpgsqlTypes.NpgsqlDbType.Double);
                                    cmd2.Parameters.Add("qty_demand", NpgsqlTypes.NpgsqlDbType.Double);
                                    cmd2.Parameters.Add("dat_creat", NpgsqlTypes.NpgsqlDbType.Date);
                                    cmd2.Parameters.Add("chksum", NpgsqlTypes.NpgsqlDbType.Integer);
                                    cmd2.Parameters.Add("id", NpgsqlTypes.NpgsqlDbType.Uuid);
                                    cmd2.Prepare();
                                    foreach (DataRow rA in rwA)
                                    {
                                        for (int i = 0; i < 24; i++)
                                        {
                                            cmd2.Parameters[i].Value = rA[i];
                                        }
                                        cmd2.ExecuteNonQuery();
                                    }
                                    Log("END ADD LACK:" + (DateTime.Now - start));
                                }
                                TR_inso_lack.Commit();
                            }
                        }
                    }
                    using (NpgsqlConnection conA = new NpgsqlConnection(npA))
                    {
                        await conA.OpenAsync();
                        using (NpgsqlTransaction Tran_formatk = conA.BeginTransaction())
                        {
                            using (NpgsqlCommand cmd = new NpgsqlCommand("REFRESH MATERIALIZED VIEW formatka; ", conA))
                            {
                                cmd.ExecuteNonQuery();
                            }
                            using (NpgsqlCommand cmd1 = new NpgsqlCommand("" +
                                "UPDATE public.datatbles " +
                                "SET last_modify=current_timestamp, in_progress=false,updt_errors=false " +
                                "WHERE table_name='ord_lack'", conA))
                            {
                                cmd1.ExecuteNonQuery();
                            }
                            Tran_formatk.Commit();
                            conA.Close();
                        }
                    }
                }
                Log("Prepare lack_report");
                using (NpgsqlConnection conB = new NpgsqlConnection(npA))
                {
                    conB.Open();
                    using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                        "select cast(count(table_name) as integer) busy " +
                        "from public.datatbles " +
                        "where (table_name='ord_lack') and in_progress=true", conB))
                    {
                        int busy_il = 1;
                        while (busy_il > 0)
                        {
                            busy_il = Convert.ToInt16(cmd.ExecuteScalar());
                            if (busy_il > 0) { System.Threading.Thread.Sleep(250); }
                        }
                    }
                }
                using (DataTable braki_pods = new DataTable())
                {
                    using (DataTable braki = new DataTable())
                    {
                        using (OracleConnection conO = new OracleConnection("Password = pass;User ID = user; Data Source = prod8"))
                        {
                            await conO.OpenAsync();
                            using (OracleCommand kal = new OracleCommand("" +
                                "SELECT ifsapp.work_time_calendar_api.Get_Work_Day_Counter ('SITS',work_day)||'_'||typ||'_'||wrkc||'_'||next_wrkc id,a.* " +
                                "FROM " +
                                    "(SELECT Decode(Sign(REVISED_DUE_DATE-SYSDATE),'-1',To_Date(SYSDATE),REVISED_DUE_DATE) WORK_DAY,Decode(source,'','MRP','DOP') TYP," +
                                    "ifsapp.shop_order_operation_list_api.Get_Next_Op_Work_Center(ORDER_NO, SEQUENCE_NO, RELEASE_NO, " +
                                        "ifsapp.shop_order_operation_list_api.Get_Prev_Non_Parallel_Op(ORDER_NO, SEQUENCE_NO, RELEASE_NO, 1, 0)) WRKC," +
                                    "ifsapp.shop_order_operation_list_api.Get_Next_Op_Work_Center(ORDER_NO, SEQUENCE_NO, RELEASE_NO, " +
                                        "ifsapp.shop_order_operation_list_api.Get_Prev_Non_Parallel_Op(ORDER_NO, SEQUENCE_NO, RELEASE_NO, 2, 0)) NEXT_WRKC," +
                                    "Sum(REVISED_QTY_DUE-QTY_COMPLETE) QTY_ALL " +
                                    "FROM ifsapp.shop_ord " +
                                    "WHERE OBJSTATE<>(select ifsapp.SHOP_ORD_API.FINITE_STATE_ENCODE__('Zamknięte') from dual) AND OBJSTATE<>(select ifsapp.SHOP_ORD_API.FINITE_STATE_ENCODE__('Wstrzymany') from dual) " +
                                    "AND REVISED_DUE_DATE < (select ifsapp.work_time_calendar_api.Get_End_Date(ifsapp.site_api.Get_Manuf_Calendar_Id('ST'), To_Date(SYSDATE), 10) from dual) " +
                                    "GROUP BY Decode(Sign(REVISED_DUE_DATE - SYSDATE), '-1', To_Date(SYSDATE), REVISED_DUE_DATE),Decode(source, '', 'MRP', 'DOP')," +
                                    "ifsapp.shop_order_operation_list_api.Get_Next_Op_Work_Center(ORDER_NO, SEQUENCE_NO, RELEASE_NO, " +
                                        "ifsapp.shop_order_operation_list_api.Get_Prev_Non_Parallel_Op(ORDER_NO, SEQUENCE_NO, RELEASE_NO, 1, 0))," +
                                    "ifsapp.shop_order_operation_list_api.Get_Next_Op_Work_Center(ORDER_NO, SEQUENCE_NO, RELEASE_NO, " +
                                        "ifsapp.shop_order_operation_list_api.Get_Prev_Non_Parallel_Op(ORDER_NO, SEQUENCE_NO, RELEASE_NO, 2, 0)) " +
                                    "ORDER BY work_day,typ,WRKC,NEXT_WRKC) a", conO))
                            {

                                using (OracleDataReader re = kal.ExecuteReader())
                                {
                                    braki.Load(re);
                                }
                            }
                            conO.Close();
                        }
                        using (NpgsqlConnection conA = new NpgsqlConnection(npA))
                        {
                            await conA.OpenAsync();
                            using (NpgsqlTransaction Tran_fill = conA.BeginTransaction())
                            {
                                using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                                    "UPDATE public.datatbles " +
                                    "SET start_update=current_timestamp,in_progress=true,updt_errors=false " +
                                    "WHERE table_name='day_qty_ifs'", conA))
                                {
                                    cmd.ExecuteNonQuery();
                                }
                                using (NpgsqlCommand cmd = new NpgsqlCommand("DELETE FROM public.day_qty_ifs", conA))
                                {
                                    cmd.ExecuteNonQuery();
                                }
                                using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                                    "INSERT INTO public.day_qty_ifs" +
                                    "(id,work_day, typ, wrkc, next_wrkc, qty_all) " +
                                    "VALUES " +
                                    "( @id, @work_day, @typ, @wrkc, @next_wrkc, @qty_all);", conA))
                                {
                                    cmd.Parameters.Add("id", NpgsqlTypes.NpgsqlDbType.Varchar);
                                    cmd.Parameters.Add("work_day", NpgsqlTypes.NpgsqlDbType.Date);
                                    cmd.Parameters.Add("typ", NpgsqlTypes.NpgsqlDbType.Varchar);
                                    cmd.Parameters.Add("wrkc", NpgsqlTypes.NpgsqlDbType.Varchar);
                                    cmd.Parameters.Add("next_wrkc", NpgsqlTypes.NpgsqlDbType.Varchar);
                                    cmd.Parameters.Add("qty_all", NpgsqlTypes.NpgsqlDbType.Double);
                                    cmd.Prepare();
                                    foreach (DataRowView rA in braki.DefaultView)
                                    {
                                        cmd.Parameters["id"].Value = (string)rA["id"];
                                        cmd.Parameters["work_day"].Value = (DateTime)rA["work_day"];
                                        cmd.Parameters["typ"].Value = (string)rA["typ"];
                                        cmd.Parameters["wrkc"].Value = (string)rA["wrkc"];
                                        cmd.Parameters["next_wrkc"].Value = (string)rA["next_wrkc"];
                                        cmd.Parameters["qty_all"].Value = Convert.ToDouble(rA["qty_all"]);
                                        cmd.ExecuteNonQuery();
                                    }
                                }
                                using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                                    "UPDATE public.datatbles " +
                                    "SET last_modify=current_timestamp,in_progress=false,updt_errors=false " +
                                    "WHERE table_name='day_qty_ifs'", conA))
                                {
                                    cmd.ExecuteNonQuery();
                                }
                                Tran_fill.Commit();
                                conA.Close();
                            }
                        }
                    }
                    using (NpgsqlConnection conB = new NpgsqlConnection(npA))
                    {
                        conB.Open();
                        using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                            "select cast(count(table_name) as integer) busy " +
                            "from public.datatbles " +
                            "where (table_name='day_qty_ifs') and in_progress=true", conB))
                        {
                            int busy_il = 1;
                            while (busy_il > 0)
                            {
                                busy_il = Convert.ToInt16(cmd.ExecuteScalar());
                                if (busy_il > 0) { System.Threading.Thread.Sleep(250); }
                            }
                        }
                    }
                    using (NpgsqlConnection conA = new NpgsqlConnection(npA))
                    {
                        await conA.OpenAsync();
                        using (NpgsqlTransaction Tran_Fill_day_qty = conA.BeginTransaction())
                        {
                            using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                                "UPDATE public.datatbles " +
                                "SET start_update=current_timestamp,in_progress=true,updt_errors=false " +
                                "WHERE table_name='day_qty'", conA))
                            {
                                cmd.ExecuteNonQuery();
                            }
                            using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                                "select a.id,a.work_day,a.typ,a.wrkc,a.next_wrkc,a.qty_all,coalesce(b.qty,0) brak " +
                                "from day_qty_ifs a " +
                                "left join " +
                                "(select wrk_count(work_day)||'_'||typ||'_'||wrkc||'_'||next_wrkc id,work_day,typ,wrkc,next_wrkc,sum(prod_qty) qty " +
                                "from (select min(a.work_day) work_day,b.order_no,b.typ,b.wrkc,b.next_wrkc,b.prod_qty " +
                                    "from (select part_no,case when work_day<current_date then current_date else work_day end work_day from demands where bal_stock<0) a," +
                                    "(select part_no,order_no,case when ord_date<current_date then current_date else ord_date end date_required,case when dop=0 then 'MRP' else 'DOP' end typ,prod_qty," +
                                    "case when wrkc=' ' then ' - ' else wrkc end wrkc,case when next_wrkc=' ' then ' - ' else next_wrkc end next_wrkc from ord_lack) b " +
                                    "where b.part_no=a.part_no and a.work_day<date_fromnow(10) and b.date_required=a.work_day " +
                                "group by b.order_no,b.typ,b.wrkc,b.next_wrkc,b.prod_qty) a " +
                                "group by work_day,typ,wrkc,next_wrkc order by work_day,typ,wrkc,next_wrkc) b " +
                                "on b.id=a.id", conA))
                            {
                                using (NpgsqlDataReader po = cmd.ExecuteReader())
                                {
                                    braki_pods.Load(po);
                                }
                            }
                            Log("UPDATE table of lack day_qty");
                            using (DataTable day_qty = new DataTable())
                            {
                                using (NpgsqlCommand cmd = new NpgsqlCommand("DELETE FROM public.day_qty where work_day<current_date", conA))
                                {
                                    cmd.ExecuteNonQuery();
                                }
                                using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                                    "select wrk_count(work_day)||'_'||typ||'_'||wrkc||'_'||next_wrkc id,day_qty.* " +
                                    "from day_qty order by work_day,typ,wrkc,next_wrkc ", conA))
                                {
                                    using (NpgsqlDataReader po = cmd.ExecuteReader())
                                    {
                                        day_qty.Load(po);
                                        int max_cont = day_qty.Rows.Count;
                                        using (DataTable change_work_day = day_qty.Clone())
                                        {
                                            change_work_day.Columns.Add("stat");
                                            var add_tbl = from ora in braki_pods.AsEnumerable()
                                                          join pst in day_qty.AsEnumerable()
                                                            on ora["id"] equals pst["id"] into prodGroup
                                                          from item in prodGroup.DefaultIfEmpty()
                                                          select new
                                                          {
                                                              id = ora["id"],
                                                              work_day = ora["work_day"],
                                                              typ = ora["typ"],
                                                              wrkc = ora["wrkc"],
                                                              next_wrkc = ora["next_wrkc"],
                                                              qty_all = ora["qty_all"],
                                                              brak = ora["brak"],
                                                              stat = item == null ? "ADD" : ora["qty_all"] != item["qty_all"] || ora["brak"] != item["brak"] ? "MOD" : "OK"
                                                          };
                                            var era_tbl = from pst in day_qty.AsEnumerable()
                                                          join ora in braki_pods.AsEnumerable()
                                                            on pst["id"] equals ora["id"] into prodGroup
                                                          from item in prodGroup.DefaultIfEmpty()
                                                          select new
                                                          {
                                                              id = pst["id"],
                                                              work_day = pst["work_day"],
                                                              typ = pst["typ"],
                                                              wrkc = pst["wrkc"],
                                                              next_wrkc = pst["next_wrkc"],
                                                              qty_all = pst["qty_all"],
                                                              brak = pst["brak"],
                                                              stat = item == null ? "ERA" : "OK"
                                                          };
                                            foreach (var ite in add_tbl)
                                            {
                                                if (ite.stat != "OK")
                                                {
                                                    change_work_day.Rows.Add(ite.id, ite.work_day, ite.typ, ite.wrkc, ite.next_wrkc, ite.qty_all, ite.brak, ite.stat);
                                                }
                                            }
                                            foreach (var ite in era_tbl)
                                            {
                                                if (ite.stat != "OK")
                                                {
                                                    change_work_day.Rows.Add(ite.id, ite.work_day, ite.typ, ite.wrkc, ite.next_wrkc, ite.qty_all, ite.brak, ite.stat);
                                                }
                                            }
                                            DataRow[] rwA = change_work_day.Select("Stat='ADD'");
                                            Log("RECORDS LACK add: " + rwA.Length);
                                            if (rwA.Length > 0)
                                            {
                                                using (NpgsqlCommand cmd2 = new NpgsqlCommand("" +
                                                    "INSERT INTO public.day_qty" +
                                                    "(work_day, typ, wrkc, next_wrkc, qty_all, brak) " +
                                                    "VALUES " +
                                                    "(@work_day, @typ, @wrkc, @next_wrkc, @qty_all, @brak)", conA))
                                                {
                                                    cmd2.Parameters.Add("work_day", NpgsqlTypes.NpgsqlDbType.Date);
                                                    cmd2.Parameters.Add("typ", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                    cmd2.Parameters.Add("wrkc", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                    cmd2.Parameters.Add("next_wrkc", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                    cmd2.Parameters.Add("qty_all", NpgsqlTypes.NpgsqlDbType.Double);
                                                    cmd2.Parameters.Add("brak", NpgsqlTypes.NpgsqlDbType.Double);
                                                    cmd2.Prepare();
                                                    foreach (DataRow rA in rwA)
                                                    {
                                                        cmd2.Parameters["work_day"].Value = (DateTime)rA["work_day"];
                                                        cmd2.Parameters["typ"].Value = (string)rA["typ"];
                                                        cmd2.Parameters["wrkc"].Value = (string)rA["wrkc"];
                                                        cmd2.Parameters["next_wrkc"].Value = (string)rA["next_wrkc"];
                                                        cmd2.Parameters["qty_all"].Value = Convert.ToDouble(rA["qty_all"]);
                                                        cmd2.Parameters["brak"].Value = Convert.ToDouble(rA["qty_all"]);
                                                        cmd2.ExecuteNonQuery();
                                                    }

                                                }
                                            }
                                            rwA = change_work_day.Select("Stat='MOD'");
                                            Log("RECORDS LACK mod: " + rwA.Length);
                                            if (rwA.Length > 0)
                                            {
                                                using (NpgsqlCommand cmd2 = new NpgsqlCommand("" +
                                                    "UPDATE public.day_qty " +
                                                    "SET qty_all=@qty_all,brak=@brak " +
                                                    "where work_day=@work_day and typ=@typ and wrkc=@wrkc and next_wrkc=@next_wrkc", conA))
                                                {
                                                    cmd2.Parameters.Add("work_day", NpgsqlTypes.NpgsqlDbType.Date);
                                                    cmd2.Parameters.Add("typ", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                    cmd2.Parameters.Add("wrkc", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                    cmd2.Parameters.Add("next_wrkc", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                    cmd2.Parameters.Add("qty_all", NpgsqlTypes.NpgsqlDbType.Double);
                                                    cmd2.Parameters.Add("brak", NpgsqlTypes.NpgsqlDbType.Double);
                                                    cmd2.Prepare();
                                                    foreach (DataRow rA in rwA)
                                                    {
                                                        cmd2.Parameters["work_day"].Value = (DateTime)rA["work_day"];
                                                        cmd2.Parameters["typ"].Value = (string)rA["typ"];
                                                        cmd2.Parameters["wrkc"].Value = (string)rA["wrkc"];
                                                        cmd2.Parameters["next_wrkc"].Value = (string)rA["next_wrkc"];
                                                        cmd2.Parameters["qty_all"].Value = Convert.ToDouble(rA["qty_all"]);
                                                        cmd2.Parameters["brak"].Value = Convert.ToDouble(rA["brak"]);
                                                        cmd2.ExecuteNonQuery();
                                                    }
                                                }
                                            }
                                            rwA = change_work_day.Select("Stat='ERA'");
                                            Log("RECORDS LACK del: " + rwA.Length);
                                            if (rwA.Length > 0)
                                            {
                                                using (NpgsqlCommand cmd2 = new NpgsqlCommand("" +
                                                    "DELETE " +
                                                    "FROM public.day_qty " +
                                                    "WHERE wrk_count(work_day)||'_'||typ||'_'||wrkc||'_'||next_wrkc=@id", conA))
                                                {
                                                    cmd2.Parameters.Add("id", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                    cmd2.Prepare();
                                                    foreach (DataRow rA in rwA)
                                                    {
                                                        cmd2.Parameters["id"].Value = (string)rA["id"];
                                                        cmd2.ExecuteNonQuery();
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                                "UPDATE public.datatbles " +
                                "SET  last_modify=current_timestamp,in_progress=false,updt_errors=false " +
                                "WHERE table_name='day_qty'", conA))
                            {
                                cmd.ExecuteNonQuery();
                            }
                            Tran_Fill_day_qty.Commit();
                        }
                        using (NpgsqlConnection conB = new NpgsqlConnection(npA))
                        {
                            conB.Open();
                            using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                                "select cast(count(table_name) as integer) busy " +
                                "from public.datatbles " +
                                "where (table_name='day_qty') and in_progress=true", conB))
                            {
                                int busy_il = 1;
                                while (busy_il > 0)
                                {
                                    busy_il = Convert.ToInt16(cmd.ExecuteScalar());
                                    if (busy_il > 0) { System.Threading.Thread.Sleep(250); }
                                }
                            }
                        }
                        using (NpgsqlTransaction Tr_brak = conA.BeginTransaction())
                        {
                            using (NpgsqlCommand cmd = new NpgsqlCommand("REFRESH MATERIALIZED VIEW braki_gniazd; ", conA))
                            {
                                cmd.ExecuteNonQuery();
                            }
                            using (NpgsqlCommand cmd = new NpgsqlCommand("REFRESH MATERIALIZED VIEW braki_poreal; ", conA))
                            {
                                cmd.ExecuteNonQuery();
                            }
                            Tr_brak.Commit();
                        }
                        conA.Close();
                        GC.Collect();
                    }
                }
                Log("END ADD WORKER:" + (DateTime.Now - start));
                return 0;
            }
            catch (Exception e)
            {
                Log("ERORR in LACK RPT" + e);
                return 1;
            }
        }
        private async Task<int> Refr_wrkc()
        {
            try
            {
                Log("Start_update WRK_CENTER");
                using (DataTable ifs_wrkc = new DataTable())
                {
                    using (OracleConnection conO = new OracleConnection("Password = pass;User ID = user; Data Source = prod8"))
                    {
                        await conO.OpenAsync();
                        using (OracleCommand kal = new OracleCommand("" +
                            "SELECT CAST (Decode(LENGTH(TRIM(TRANSLATE(ORDER_NO, ' +-.0123456789',' '))),NULL,ORDER_NO,owa_opt_lock.checksum(ORDER_NO)*-1) AS INTEGER) int_ord," +
                            "ifsapp.shop_order_operation_list_api.Get_Next_Op_Work_Center(ORDER_NO,SEQUENCE_NO,RELEASE_NO, ifsapp.shop_order_operation_list_api.Get_Prev_Non_Parallel_Op(ORDER_NO,SEQUENCE_NO,RELEASE_NO, 1, 0)) WRKC," +
                            "ifsapp.shop_order_operation_list_api.Get_Next_Op_Work_Center(ORDER_NO,SEQUENCE_NO,RELEASE_NO, ifsapp.shop_order_operation_list_api.Get_Prev_Non_Parallel_Op(ORDER_NO,SEQUENCE_NO,RELEASE_NO, 2, 0)) NEXT_WRKC," +
                            "REVISED_QTY_DUE-QTY_COMPLETE QTY_ALL " +
                            "FROM ifsapp.shop_ord " +
                            "WHERE OBJSTATE <> (select ifsapp.SHOP_ORD_API.FINITE_STATE_ENCODE__('Zamknięte') from dual) AND OBJSTATE <> (select ifsapp.SHOP_ORD_API.FINITE_STATE_ENCODE__('Wstrzymany') from dual) " +
                            "AND REVISED_DUE_DATE<(select ifsapp.work_time_calendar_api.Get_End_Date(ifsapp.site_api.Get_Manuf_Calendar_Id('ST'),To_Date(SYSDATE),10) from dual) " +
                            "ORDER BY int_ord", conO))
                        {
                            using (OracleDataReader re = kal.ExecuteReader())
                            {
                                ifs_wrkc.Load(re);
                            }
                        }
                        conO.Close();
                    }
                    using (DataTable pstgr_wrkc = new DataTable())
                    {
                        using (NpgsqlConnection conA = new NpgsqlConnection(npA))
                        {
                            await conA.OpenAsync();
                            using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                                "select int_ord,wrkc,next_wrkc,prod_qty " +
                                "from ord_demands " +
                                "where order_supp_dmd='Rez. mat. ZP' and date_required<date_fromnow(10) " +
                                "group by int_ord,wrkc,next_wrkc,prod_qty order by int_ord ", conA))
                            {
                                using (NpgsqlDataReader po = cmd.ExecuteReader())
                                {
                                    pstgr_wrkc.Load(po);
                                    pstgr_wrkc.Columns.Add("STAT");
                                }
                            }
                            conA.Close();
                            int ind_ifs = 0;
                            int max_indifs = ifs_wrkc.Rows.Count - 1;
                            foreach (DataRowView rek in pstgr_wrkc.DefaultView)
                            {
                                if (ind_ifs < max_indifs)
                                {
                                    while (Convert.ToInt32(rek["int_ord"]) > Convert.ToInt32(ifs_wrkc.DefaultView[ind_ifs].Row["int_ord"]))
                                    {
                                        ind_ifs++;
                                        if (ind_ifs >= max_indifs) { break; }
                                    }
                                    if (Convert.ToInt32(rek["int_ord"]) == Convert.ToInt32(ifs_wrkc.DefaultView[ind_ifs].Row["int_ord"]))
                                    {
                                        if ((string)rek["wrkc"] != (string)ifs_wrkc.DefaultView[ind_ifs].Row["wrkc"] || (string)rek["next_wrkc"] != (string)ifs_wrkc.DefaultView[ind_ifs].Row["next_wrkc"] || Convert.ToDouble(rek["prod_qty"]) != Convert.ToDouble(ifs_wrkc.DefaultView[ind_ifs].Row["qty_all"]))
                                        {
                                            rek["STAT"] = "MOD";
                                            rek["wrkc"] = (string)ifs_wrkc.DefaultView[ind_ifs].Row["wrkc"];
                                            rek["next_wrkc"] = (string)ifs_wrkc.DefaultView[ind_ifs].Row["next_wrkc"];
                                            rek["prod_qty"] = Convert.ToDouble(ifs_wrkc.DefaultView[ind_ifs].Row["qty_all"]);
                                        }
                                    }
                                }
                            }
                            DataRow[] new_rek = pstgr_wrkc.Select("stat='MOD'");
                            if (new_rek.Length > 0)
                            {
                                await conA.OpenAsync();
                                using (NpgsqlTransaction TR_WRKC = conA.BeginTransaction())
                                {
                                    using (NpgsqlCommand cmd1 = new NpgsqlCommand("" +
                                        "UPDATE public.ord_demands " +
                                        "set wrkc=@wrkc, next_wrkc=@next_wrkc, prod_qty=@prod_qty " +
                                        "where int_ord=@int_ord and order_supp_dmd='Rez. mat. ZP' ", conA))
                                    {
                                        cmd1.Parameters.Add("wrkc", NpgsqlTypes.NpgsqlDbType.Varchar);
                                        cmd1.Parameters.Add("next_wrkc", NpgsqlTypes.NpgsqlDbType.Varchar);
                                        cmd1.Parameters.Add("int_ord", NpgsqlTypes.NpgsqlDbType.Integer);
                                        cmd1.Parameters.Add("prod_qty", NpgsqlTypes.NpgsqlDbType.Double);
                                        cmd1.Prepare();
                                        foreach (DataRow row in new_rek)
                                        {
                                            cmd1.Parameters["wrkc"].Value = (string)row["wrkc"];
                                            cmd1.Parameters["next_wrkc"].Value = (string)row["next_wrkc"];
                                            cmd1.Parameters["int_ord"].Value = Convert.ToInt32(row["int_ord"]);
                                            cmd1.Parameters["prod_qty"].Value = Convert.ToDouble(row["prod_qty"]);
                                            cmd1.ExecuteNonQuery();
                                        }
                                    }
                                    TR_WRKC.Commit();
                                }
                                conA.Close();
                            }
                        }
                    }
                }
                GC.Collect();
                Log("END WRK_CENTER");
                return 0;
            }
            catch (Exception e)
            {
                Log("Error in update wrkCNTER:" + e);
                return 1;
            }
        }
        private async Task<int> Modify_prod_date()
        {
            try
            {
                using (NpgsqlConnection conA = new NpgsqlConnection(npA))
                {
                    await conA.OpenAsync();
                    using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                        "select cast(count(table_name) as integer) busy " +
                        "from public.datatbles " +
                        "where substring(table_name,1,7)='cal_ord' and in_progress=true", conA))
                    {
                        int busy_il = 1;
                        while (busy_il > 0)
                        {
                            busy_il = Convert.ToInt16(cmd.ExecuteScalar());
                            if (busy_il > 0) { System.Threading.Thread.Sleep(250); }
                        }
                    }
                    conA.Close();
                }
                int ra = await Send_mail_lack();
                Log("Start modyfing prod date in ORD_DOP:" + (DateTime.Now - start));
                using (DataTable ord_mod = new DataTable())
                {
                    using (NpgsqlConnection conA = new NpgsqlConnection(npA))
                    {
                        await conA.OpenAsync();
                        //using (NpgsqlCommand cmd = new NpgsqlCommand("select order_no,line_no,rel_no,prod from mail where status_informacji='WYKONANIE' or (info_handlo=false and logistyka=false and status_informacji!='POPRAWIĆ')", conA))
                        using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                            "select a.*,get_date_dop(a.cust_id) data_dop " +
                            "from mail a " +
                            "left join " +
                            "(select cust_id,max(date_addd) from mail_hist group by cust_id having max(date_addd)>current_timestamp - interval '10 minute') b " +
                            "on b.cust_id=a.cust_id " +
                            "where b.cust_id is null and a.prod>=current_date and ((a.status_informacji='WYKONANIE' and a.info_handlo=false) " +
                                "or (is_confirm(a.status_informacji)=true and get_date_dop(a.cust_id)!=prod and a.seria0=false) or (is_alter(a.status_informacji)=true and get_date_dop(a.cust_id)!=prod ) " +
                                "and a.info_handlo=false) and get_dopstat(a.cust_id) is not null", conA))
                        {
                            using (NpgsqlDataReader po = cmd.ExecuteReader())
                            {
                                ord_mod.Load(po);
                            }
                        }
                        conA.Close();
                    }
                    using (OracleConnection conO = new OracleConnection("Password=pass;User ID = user; Data Source = prod8"))
                    {
                        await conO.OpenAsync();
                        OracleGlobalization info = conO.GetSessionInfo();
                        info.DateFormat = "YYYY-MM-DD";
                        conO.SetSessionInfo(info);
                        using (OracleCommand comm = new OracleCommand("ifsapp.c_customer_order_line_api.Cancel_Schedule", conO))
                        {
                            comm.CommandType = CommandType.StoredProcedure;
                            comm.Parameters.Add("order_no", OracleDbType.Varchar2);
                            comm.Parameters.Add("line_no", OracleDbType.Varchar2);
                            comm.Parameters.Add("rel_no", OracleDbType.Varchar2);
                            comm.Parameters.Add("line_item_no", OracleDbType.Decimal);
                            comm.Prepare();
                            foreach (DataRowView rek in ord_mod.DefaultView)
                            {
                                try
                                {
                                    comm.Parameters["order_no"].Value = (string)rek["order_no"];
                                    comm.Parameters["line_no"].Value = (string)rek["line_no"];
                                    comm.Parameters["rel_no"].Value = (string)rek["rel_no"];
                                    comm.Parameters["line_item_no"].Value = 0;
                                    comm.ExecuteNonQuery();
                                }
                                catch (Exception e)
                                {
                                    Log("Nie zdjęto harmonogramu " + e);
                                }
                            }
                        }
                        using (NpgsqlConnection conA = new NpgsqlConnection(npA))
                        {
                            await conA.OpenAsync();
                            //using (NpgsqlCommand cmd = new NpgsqlCommand("select order_no,line_no,rel_no,prod from mail where status_informacji='WYKONANIE' or (info_handlo=false and logistyka=false and status_informacji!='POPRAWIĆ')", conA))
                            using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                                "INSERT INTO public.mod_date" +
                                "(order_no, line_no, rel_no, prod, typ_zdarzenia, status_informacji, dop, err,indeks,opis,data_dop,date_add) " +
                                "VALUES " +
                                "(@order_no, @line_no, @rel_no, @prod, @typ_zdarzenia, @status_informacji, @dop, @err,@indeks,@opis,@data_dop,current_timestamp)", conA))
                            {
                                cmd.Parameters.Add("order_no", NpgsqlTypes.NpgsqlDbType.Varchar);
                                cmd.Parameters.Add("line_no", NpgsqlTypes.NpgsqlDbType.Varchar);
                                cmd.Parameters.Add("rel_no", NpgsqlTypes.NpgsqlDbType.Varchar);
                                cmd.Parameters.Add("prod", NpgsqlTypes.NpgsqlDbType.Date);
                                cmd.Parameters.Add("typ_zdarzenia", NpgsqlTypes.NpgsqlDbType.Varchar);
                                cmd.Parameters.Add("status_informacji", NpgsqlTypes.NpgsqlDbType.Varchar);
                                cmd.Parameters.Add("dop", NpgsqlTypes.NpgsqlDbType.Integer);
                                cmd.Parameters.Add("err", NpgsqlTypes.NpgsqlDbType.Boolean);
                                cmd.Parameters.Add("indeks", NpgsqlTypes.NpgsqlDbType.Varchar);
                                cmd.Parameters.Add("opis", NpgsqlTypes.NpgsqlDbType.Varchar);
                                cmd.Parameters.Add("data_dop", NpgsqlTypes.NpgsqlDbType.Date);
                                cmd.Prepare();
                                using (OracleCommand comm = new OracleCommand("ifsapp.c_customer_order_line_api.Set_Planned_Manuf_Date", conO))
                                {
                                    comm.CommandType = CommandType.StoredProcedure;
                                    comm.Parameters.Add("order_no", OracleDbType.Varchar2);
                                    comm.Parameters.Add("line_no", OracleDbType.Varchar2);
                                    comm.Parameters.Add("rel_no", OracleDbType.Varchar2);
                                    comm.Parameters.Add("line_item_no", OracleDbType.Decimal);
                                    comm.Parameters.Add("planned_date", OracleDbType.Date);
                                    comm.Prepare();
                                    foreach (DataRowView rek in ord_mod.DefaultView)
                                    {
                                        try
                                        {
                                            comm.Parameters["order_no"].Value = (string)rek["order_no"];
                                            comm.Parameters["line_no"].Value = (string)rek["line_no"];
                                            comm.Parameters["rel_no"].Value = (string)rek["rel_no"];
                                            comm.Parameters["line_item_no"].Value = 0;
                                            comm.Parameters["planned_date"].Value = (DateTime)rek["prod"];
                                            comm.ExecuteNonQuery();
                                            if (!(bool)rek["info_handlo"])
                                            {
                                                rek.BeginEdit();
                                                rek["ordid"] = "";
                                                rek.EndEdit();
                                            }
                                            cmd.Parameters["order_no"].Value = (string)rek["order_no"];
                                            cmd.Parameters["line_no"].Value = (string)rek["line_no"];
                                            cmd.Parameters["rel_no"].Value = (string)rek["rel_no"];
                                            cmd.Parameters["prod"].Value = (DateTime)rek["prod"];
                                            cmd.Parameters["typ_zdarzenia"].Value = (string)rek["typ_zdarzenia"];
                                            cmd.Parameters["status_informacji"].Value = (string)rek["status_informacji"];
                                            cmd.Parameters["dop"].Value = (int)rek["dop"];
                                            cmd.Parameters["err"].Value = false;
                                            cmd.Parameters["indeks"].Value = (string)rek["indeks"];
                                            cmd.Parameters["opis"].Value = (string)rek["opis"];
                                            cmd.Parameters["data_dop"].Value = (DateTime)rek["data_dop"];
                                            cmd.ExecuteNonQuery();
                                        }
                                        catch
                                        {
                                            int check_err = await Kick_off_err_orders((int)rek["dop"], (DateTime)rek["prod"]);
                                            if (check_err == 1)
                                            {
                                                rek.BeginEdit();
                                                rek["ordid"] = "";
                                                rek.EndEdit();
                                                cmd.Parameters["order_no"].Value = (string)rek["order_no"];
                                                cmd.Parameters["line_no"].Value = (string)rek["line_no"];
                                                cmd.Parameters["rel_no"].Value = (string)rek["rel_no"];
                                                cmd.Parameters["prod"].Value = (DateTime)rek["prod"];
                                                cmd.Parameters["typ_zdarzenia"].Value = (string)rek["typ_zdarzenia"];
                                                cmd.Parameters["status_informacji"].Value = (string)rek["status_informacji"];
                                                cmd.Parameters["dop"].Value = (int)rek["dop"];
                                                cmd.Parameters["err"].Value = true;
                                                cmd.Parameters["indeks"].Value = (string)rek["indeks"];
                                                cmd.Parameters["opis"].Value = (string)rek["opis"];
                                                cmd.Parameters["data_dop"].Value = (DateTime)rek["data_dop"];
                                                cmd.ExecuteNonQuery();
                                                Log("Nie przeplanowano :" + rek[0] + "_" + rek[1] + "_" + rek[2] + " na datę:" + rek[3]);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                    DataRow[] new_rek = ord_mod.Select("ordid<>''");
                    {
                        if (new_rek.Length > 0)
                        {
                            using (NpgsqlConnection conA = new NpgsqlConnection(npA))
                            {
                                await conA.OpenAsync();
                                foreach (DataRow rw in new_rek)
                                {
                                    bool chk;
                                    using (NpgsqlCommand cmd = new NpgsqlCommand("select late_ord_exist(@cust_id)", conA))
                                    {
                                        // sprawdź czy adnotacja z zamówieniem opóźnionym nie istnieje
                                        cmd.Parameters.Add("@cust_id", NpgsqlTypes.NpgsqlDbType.Uuid).Value = rw["cust_id"];
                                        cmd.Prepare();
                                        chk = Convert.ToBoolean(cmd.ExecuteScalar());
                                    }
                                    if (!chk)
                                    {
                                        using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                                            "INSERT INTO public.late_ord" +
                                            "(ordid, dop, koor, order_no, line_no, rel_no, part_no, descr, country, prom_date, prom_week, load_id, ship_date, date_entered, cust_id, prod, prod_week, planner_buyer, " +
                                            "indeks, opis, typ_zdarzenia, status_informacji, zest, info_handlo, logistyka, seria0, data0, cust_line_stat, ord_objver, data_dop) " +
                                            "VALUES " +
                                            "(@ordid, @dop, @koor, @order_no, @line_no, @rel_no, @part_no, @descr, @country, @prom_date, @prom_week, @load_id, @ship_date, @date_entered, @cust_id, @prod, @prod_week, " +
                                            "@planner_buyer, @indeks, @opis, @typ_zdarzenia, @status_informacji, @zest, @info_handlo, @logistyka, @seria0, @data0, @cust_line_stat, @ord_objver, @data_dop); ", conA))
                                        {
                                            cmd.Parameters.Add("@ordid", NpgsqlTypes.NpgsqlDbType.Text).Value = (string)rw["ordid"];
                                            cmd.Parameters.Add("@dop", NpgsqlTypes.NpgsqlDbType.Integer).Value = Convert.ToInt32(rw["dop"]);
                                            cmd.Parameters.Add("@koor", NpgsqlTypes.NpgsqlDbType.Varchar).Value = (string)rw["koor"];
                                            cmd.Parameters.Add("@order_no", NpgsqlTypes.NpgsqlDbType.Varchar).Value = (string)rw["order_no"];
                                            cmd.Parameters.Add("@line_no", NpgsqlTypes.NpgsqlDbType.Varchar).Value = (string)rw["line_no"];
                                            cmd.Parameters.Add("@rel_no", NpgsqlTypes.NpgsqlDbType.Varchar).Value = (string)rw["rel_no"];
                                            cmd.Parameters.Add("@part_no", NpgsqlTypes.NpgsqlDbType.Varchar).Value = (string)rw["part_no"];
                                            cmd.Parameters.Add("@descr", NpgsqlTypes.NpgsqlDbType.Varchar).Value = (string)rw["descr"];
                                            cmd.Parameters.Add("@country", NpgsqlTypes.NpgsqlDbType.Varchar).Value = (string)rw["country"];
                                            cmd.Parameters.Add("@prom_date", NpgsqlTypes.NpgsqlDbType.Date).Value = (DateTime)rw["prom_date"];
                                            cmd.Parameters.Add("@prom_week", NpgsqlTypes.NpgsqlDbType.Integer).Value = Convert.ToInt32(rw["prom_week"]);
                                            cmd.Parameters.Add("@load_id", NpgsqlTypes.NpgsqlDbType.Bigint).Value = rw["load_id"] ?? DBNull.Value;
                                            cmd.Parameters.Add("@ship_date", NpgsqlTypes.NpgsqlDbType.Date).Value = rw["ship_date"] ?? DBNull.Value;
                                            cmd.Parameters.Add("@date_entered", NpgsqlTypes.NpgsqlDbType.Timestamp).Value = (DateTime)rw["date_entered"];
                                            cmd.Parameters.Add("@cust_id", NpgsqlTypes.NpgsqlDbType.Uuid).Value = (Guid)rw["cust_id"];
                                            cmd.Parameters.Add("@prod", NpgsqlTypes.NpgsqlDbType.Date).Value = (DateTime)rw["prod"];
                                            cmd.Parameters.Add("@prod_week", NpgsqlTypes.NpgsqlDbType.Integer).Value = Convert.ToInt32(rw["prod_week"]);
                                            cmd.Parameters.Add("@planner_buyer", NpgsqlTypes.NpgsqlDbType.Varchar).Value = (string)rw["planner_buyer"];
                                            cmd.Parameters.Add("@indeks", NpgsqlTypes.NpgsqlDbType.Varchar).Value = (string)rw["indeks"];
                                            cmd.Parameters.Add("@opis", NpgsqlTypes.NpgsqlDbType.Varchar).Value = (string)rw["opis"];
                                            cmd.Parameters.Add("@typ_zdarzenia", NpgsqlTypes.NpgsqlDbType.Varchar).Value = (string)rw["typ_zdarzenia"];
                                            cmd.Parameters.Add("@status_informacji", NpgsqlTypes.NpgsqlDbType.Varchar).Value = (string)rw["status_informacji"];
                                            cmd.Parameters.Add("@zest", NpgsqlTypes.NpgsqlDbType.Varchar).Value = rw["zest"] ?? DBNull.Value;
                                            cmd.Parameters.Add("@info_handlo", NpgsqlTypes.NpgsqlDbType.Boolean).Value = (bool)rw["info_handlo"];
                                            cmd.Parameters.Add("@logistyka", NpgsqlTypes.NpgsqlDbType.Boolean).Value = (bool)rw["logistyka"];
                                            cmd.Parameters.Add("@seria0", NpgsqlTypes.NpgsqlDbType.Boolean).Value = (bool)rw["seria0"];
                                            cmd.Parameters.Add("@data0", NpgsqlTypes.NpgsqlDbType.Date).Value = rw["data0"] ?? DBNull.Value;
                                            cmd.Parameters.Add("@cust_line_stat", NpgsqlTypes.NpgsqlDbType.Varchar).Value = (string)rw["cust_line_stat"];
                                            cmd.Parameters.Add("@ord_objver", NpgsqlTypes.NpgsqlDbType.Timestamp).Value = (DateTime)rw["ord_objver"];
                                            cmd.Parameters.Add("@data_dop", NpgsqlTypes.NpgsqlDbType.Date).Value = (DateTime)rw["data_dop"];
                                            cmd.Prepare();
                                            await cmd.ExecuteNonQueryAsync();
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                Log("END modyfing prod date in ORD_DOP:" + (DateTime.Now - start));
                return 0;
            }
            catch (Exception e)
            {
                Log("Błąd modyfikacji daty produkcji:" + e);
                return 1;
            }
        }
        private async Task<int> Confirm_ORd()
        {
            try
            {
                using (DataTable conf_addr = new DataTable())
                {
                    using (NpgsqlConnection conA = new NpgsqlConnection(npA))
                    {
                        conA.Open();
                        using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                            "select cast(count(table_name) as integer) busy " +
                            "from public.datatbles " +
                            "where  table_name='TR_sendm' and in_progress=true", conA))
                        {
                            int busy_il = 1;
                            while (busy_il > 0)
                            {
                                busy_il = Convert.ToInt16(cmd.ExecuteScalar());
                                if (busy_il > 0) { System.Threading.Thread.Sleep(250); }
                            }
                        }
                        using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                            "DELETE FROM public.conf_mail_null " +
                            "WHERE order_no not in (SELECT a.order_no  " +
                                "FROM ( SELECT a_1.*,get_refer(a_1.addr1) AS reference,CASE WHEN a_1.dop_connection_db::text = 'AUT'::text AND a_1.dop_state IS NULL THEN 1 ELSE 0 END AS gotowe " +
                                    "FROM cust_ord a_1 " +
                                    "LEFT JOIN " +
                                    "( SELECT cust_ord.order_no " +
                                        "FROM cust_ord " +
                                        "WHERE cust_ord.state_conf::text = 'Wydrukow.'::text AND cust_ord.last_mail_conf IS NOT NULL " +
                                        "GROUP BY cust_ord.order_no) c " +
                                    "ON c.order_no::text = a_1.order_no::text " +
                                    "WHERE (a_1.state_conf::text = 'Nie wydruk.'::text OR a_1.last_mail_conf IS NULL) " +
                                        "AND is_refer(a_1.addr1) = true AND substring(a_1.order_no::text, 1, 1) = 'S'::text " +
                                        "AND (a_1.cust_order_state::text <> ALL (ARRAY['Częściowo dostarczone'::character varying::text, 'Zaplanowane'::character varying::text])) " +
                                        "AND (substring(a_1.part_no::text, 1, 3) <> ALL (ARRAY['633'::text, '628'::text, '1K1'::text, '1U2'::text, '632'::text])) " +
                                        "AND (c.order_no IS NOT NULL AND a_1.dop_connection_db::text <> 'MAN'::text OR c.order_no IS NULL)) a " +
                                    "GROUP BY a.order_no, a.cust_no, a.reference, a.addr1, a.country " +
                                    "HAVING sum(a.gotowe) = 0 AND max(a.objversion) < (now() - '02:00:00'::interval))", conA))
                        {
                            cmd.ExecuteNonQuery();
                        }
                        using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                            "select a.order_no,a.cust_no,a.reference,cast('' as varchar )mail,a.country,current_date as date_add " +
                            "from confirm_ord a " +
                            "order by cust_no,reference,order_no", conA))
                        {
                            using (NpgsqlDataReader po = cmd.ExecuteReader())
                            {
                                conf_addr.Load(po);
                            }
                        }
                    }
                    if (conf_addr.Rows.Count > 0)
                    {
                        using (OracleConnection conO = new OracleConnection("Password=pass;User ID = user; Data Source = prod8"))
                        {
                            conO.Open();
                            OracleGlobalization info = conO.GetSessionInfo();
                            info.DateFormat = "YYYY-MM-DD";
                            conO.SetSessionInfo(info);
                            using (OracleCommand comm = new OracleCommand("" +
                                "SELECT ifsapp.Comm_Method_API.Get_Name_Value('CUSTOMER',:cust_no,'E_MAIL',:reference, ifsapp.customer_order_api.Get_C_Final_Addr_No(:order_no),SYSDATE)  " +
                                "FROM dual", conO))
                            {
                                comm.Parameters.Add(":cust_no", OracleDbType.Varchar2);
                                comm.Parameters.Add(":reference", OracleDbType.Varchar2);
                                comm.Parameters.Add(":order_no", OracleDbType.Varchar2);
                                comm.Prepare();
                                foreach (DataRow rw in conf_addr.Rows)
                                {
                                    comm.Parameters[0].Value = rw["cust_no"];
                                    comm.Parameters[1].Value = rw["reference"];
                                    comm.Parameters[2].Value = rw["order_no"];
                                    string mail = Convert.ToString(comm.ExecuteScalar());
                                    rw[3] = mail;
                                }
                            }
                        }
                        DataRow[] nullmail = conf_addr.Select("mail=''");
                        if (nullmail.Length > 0)
                        {
                            using (NpgsqlConnection conA = new NpgsqlConnection(npA))
                            {
                                await conA.OpenAsync();
                                using (NpgsqlCommand cmd = new NpgsqlCommand("select date_add from conf_mail_null where order_no=@order_no", conA))
                                {
                                    cmd.Parameters.Add("order_no", NpgsqlTypes.NpgsqlDbType.Varchar);
                                    cmd.Prepare();
                                    foreach (DataRow rw in nullmail)
                                    {
                                        using (DataTable reko = new DataTable())
                                        {
                                            cmd.Parameters[0].Value = rw[0];
                                            using (NpgsqlDataReader po = cmd.ExecuteReader())
                                            {
                                                reko.Load(po);
                                            }
                                            if (reko.Rows.Count == 0)
                                            {
                                                using (NpgsqlConnection cob = new NpgsqlConnection(npA))
                                                {
                                                    await cob.OpenAsync();
                                                    using (NpgsqlCommand cm = new NpgsqlCommand("" +
                                                        "INSERT " +
                                                        "INTO public.conf_mail_null" +
                                                        "(order_no, cust_no, reference, country) " +
                                                        "VALUES " +
                                                        "(@rw1,@rw2,@rw3,@rw4)", cob))
                                                    {
                                                        cm.Parameters.Add("rw1", NpgsqlTypes.NpgsqlDbType.Varchar).Value = rw[0];
                                                        cm.Parameters.Add("rw2", NpgsqlTypes.NpgsqlDbType.Varchar).Value = rw[1];
                                                        cm.Parameters.Add("rw3", NpgsqlTypes.NpgsqlDbType.Varchar).Value = rw[2];
                                                        cm.Parameters.Add("rw4", NpgsqlTypes.NpgsqlDbType.Varchar).Value = rw[4];
                                                        cm.Prepare();
                                                        cm.ExecuteNonQuery();
                                                    }
                                                }
                                            }
                                            else
                                            {
                                                DataRow ra = reko.Rows[0];
                                                if (!ra.IsNull("date_add"))
                                                {
                                                    if ((DateTime)ra["date_add"] != DateTime.Now.Date)
                                                    {
                                                        using (NpgsqlConnection cob = new NpgsqlConnection(npA))
                                                        {
                                                            await cob.OpenAsync();
                                                            using (NpgsqlCommand cm = new NpgsqlCommand("" +
                                                                "UPDATE public.conf_mail_null " +
                                                                "SET cust_no=@rw1, reference=@rw2, mail=null, country=@rw3, date_add=null	" +
                                                                "WHERE order_no=@rw4;", cob))
                                                            {
                                                                cm.Parameters.Add("rw1", NpgsqlTypes.NpgsqlDbType.Varchar).Value = rw[1];
                                                                cm.Parameters.Add("rw2", NpgsqlTypes.NpgsqlDbType.Varchar).Value = rw[2];
                                                                cm.Parameters.Add("rw3", NpgsqlTypes.NpgsqlDbType.Varchar).Value = rw[4];
                                                                cm.Parameters.Add("rw4", NpgsqlTypes.NpgsqlDbType.Varchar).Value = rw[0];
                                                                cm.Prepare();
                                                                cm.ExecuteNonQuery();
                                                            }
                                                        }
                                                    }
                                                }

                                            }
                                        }
                                    }

                                }
                                using (DataTable mail_list = new DataTable())
                                {
                                    using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                                        "select b.mail " +
                                        "from " +
                                            "(select * from " +
                                            "conf_mail_null " +
                                            "where date_add is null) a," +
                                            "kontakty b " +
                                        "where b.country_coor=a.country " +
                                        "group by b.mail", conA))
                                    {
                                        using (NpgsqlDataReader po = cmd.ExecuteReader())
                                        {
                                            mail_list.Load(po);
                                        }
                                    }
                                    if (mail_list.Rows.Count > 0)
                                    {
                                        using (DataTable kol = new DataTable())
                                        {
                                            using (NpgsqlCommand pot = new NpgsqlCommand("" +
                                                "select order_no as cust_ord,cust_no,reference,country " +
                                                "from conf_mail_null", conA))
                                            {
                                                using (NpgsqlDataReader po = pot.ExecuteReader())
                                                {
                                                    using (DataTable sch = po.GetSchemaTable())
                                                    {
                                                        foreach (DataRow a in sch.Rows)
                                                        {
                                                            kol.Columns.Add(a["ColumnName"].ToString().ToUpper(), System.Type.GetType("System.Int32"));
                                                        }
                                                    }
                                                }
                                            }
                                            DataRow rw = kol.NewRow();
                                            for (int i = 0; i < kol.Columns.Count; i++)
                                            {
                                                rw[i] = 0;
                                            }
                                            rw["cust_ord"] = 1;
                                            rw["reference"] = 1;
                                            kol.Rows.Add(rw);
                                            using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                                                "select a.order_no as cust_ord,a.cust_no,a.reference,a.country,' ' info " +
                                                "from " +
                                                "(select * from conf_mail_null where date_add is null) a," +
                                                "kontakty b " +
                                                "where b.country_coor=a.country and b.mail=@mail ", conA))
                                            {
                                                cmd.Parameters.Add("mail", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                cmd.Prepare();
                                                foreach (DataRow rek in mail_list.Rows)
                                                {
                                                    cmd.Parameters[0].Value = rek[0];
                                                    using (NpgsqlDataReader re = cmd.ExecuteReader())
                                                    {
                                                        using (DataTable mal = new DataTable())
                                                        {
                                                            mal.Load(re);
                                                            if (mal.Rows.Count > 0)
                                                            {
                                                                int send = await Create_HTMLmail(mal, "Brak ustalonego adresu e-mail dla potwierdzenia zamówienia", rek[0].ToString().Replace("\r", ""), kol.Rows[0], "*Powyższe zamówienia nie mogą zostać potwierdzone do czasu wprowadzenia brakujących informacji w kartotece klienta");
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                        using (NpgsqlCommand cmd = new NpgsqlCommand("UPDATE public.conf_mail_null SET date_add=current_date WHERE date_add is null", conA))
                                        {
                                            cmd.ExecuteNonQuery();
                                        }
                                    }
                                }
                            }
                        }
                        using (OracleConnection conO = new OracleConnection("Password=pass;User ID = user; Data Source = prod8"))
                        {
                            conO.Open();
                            OracleGlobalization info = conO.GetSessionInfo();
                            info.DateFormat = "YYYY-MM-DD";
                            conO.SetSessionInfo(info);
                            using (OracleCommand comm = new OracleCommand("ifsapp.Customer_Order_Flow_API.Email_Order_Report__", conO))
                            {
                                comm.CommandType = CommandType.StoredProcedure;
                                comm.Parameters.Add("order_no", OracleDbType.Varchar2);
                                comm.Parameters.Add("refer", OracleDbType.Varchar2);
                                comm.Parameters.Add("lok", OracleDbType.Varchar2);
                                comm.Parameters.Add("mail", OracleDbType.Varchar2);
                                comm.Parameters.Add("cust_no", OracleDbType.Varchar2);
                                comm.Parameters.Add("rep_nam", OracleDbType.Varchar2);
                                comm.Prepare();
                                foreach (DataRow rek in conf_addr.Rows)
                                {
                                    if (!rek.IsNull("mail"))
                                    {
                                        if (rek["mail"].ToString().Length > 5)
                                        {
                                            {
                                                comm.Parameters[0].Value = (string)rek["order_no"];
                                                comm.Parameters[1].Value = (string)rek["reference"];
                                                comm.Parameters[2].Value = "ST";
                                                comm.Parameters[3].Value = (string)rek["mail"];
                                                comm.Parameters[4].Value = (string)rek["cust_no"];
                                                comm.Parameters[5].Value = "CUSTOMER_ORDER_CONF_REP";
                                                comm.ExecuteNonQuery();
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                return 0;
            }
            catch (Exception e)
            {

                Log("Błąd" + e);
                return 1;
            }
        }
        private async Task<int> Create_HTMLmail(DataTable rek, string StrTableStart, string mailto, DataRow spec_kol, string tblfoot = "")
        {
            string txtwh = " ";
            Boolean haveshort = false;
            try
            {
                string firstbod = "<?xml version=" + (Char)34 + "1.0" + (Char)34 + " encoding =" + (Char)34 + "utf-8" + (Char)34 + " ?> <!DOCTYPE html PUBLIC " + (Char)34 + "-//W3C//DTD XHTML 1.0 Transitional//EN" + (Char)34 + " " + (Char)34 + "http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd" + (Char)34 + "> <html xmlns=" + (Char)34 + "http://www.w3.org/1999/xhtml" + (Char)34 + "> <head> <meta http-equiv=" + (Char)34 + "Content-Type" + (Char)34 + " content =" + (Char)34 + "text/html charset=UTF-8" + (Char)34 + " />  <meta name=" + (Char)34 + "viewport" + (Char)34 + " content=" + (Char)34 + "width=device-width,initial-scale=1.0" + (Char)34 + "/>  <title>" + HTMLEncode(StrTableStart) + "</title>  <style type=" + (Char)34 + "text/css" + (Char)34 + ">  body,table{font-family:verdana,arial,sans-serif;font-size:12px;border-collapse:collapse;}  td,th{margin:3px;border:1px solid #BBB;} </style> </head> <body>";
                string Shortcut = " <p> <a href=" + (Char)34 + "http://ifsvapp1.sits.local:59080/client/runtime/Ifs.Fnd.Explorer.application?url=ifsapf%3AtbwOverviewCustOrdLine%3Faction%3Dget";
                string StrTableStart1 = " <h5>WITAM<br />" + StrTableStart + "</h5> ";
                string strTableBeg = "<table>";
                string strTableEnd = "</table><i>" + tblfoot + "</i><h5>Pozdrawiam<br />Radek Kosobucki</h5>";
                string strTableHeader = "<tr bgcolor=" + (Char)34 + "lightblue" + (Char)34 + "> ";
                string lastbod = "</body></html>";
                foreach (DataColumn cl in spec_kol.Table.Columns)
                {
                    strTableHeader = strTableHeader + "<th>" + HTMLEncode(cl.ColumnName) + "</th>";
                }
                strTableHeader = strTableHeader + "</tr>";
                //Build HTML Output for the DataSet
                string strTableBody = StrTableStart1 + strTableBeg + strTableHeader;
                int ind = 1;
                bool tst = false;
                bool nowr = false;
                foreach (DataRowView row in rek.DefaultView)
                {
                    if (txtwh.IndexOf(row["cust_ord"].ToString()) < 0)
                    {
                        txtwh = txtwh + row["cust_ord"].ToString() + ";";
                    }
                    strTableBody = strTableBody + "<tr>";
                    for (int i = 0; i < spec_kol.Table.Columns.Count; i++)
                    {
                        if (spec_kol.Table.Columns[i].ColumnName == "PROD_DATE" || spec_kol.Table.Columns[i].ColumnName == "CORR") { nowr = true; }
                        if (spec_kol.Table.Columns[i].ColumnName.ToUpper() == "C_LIN") { haveshort = true; }
                        if (Convert.ToDouble(spec_kol[i]) == 1) { tst = true; }
                        strTableBody = strTableBody + TD(HTMLEncode(row[i].ToString()), tst, tst, nowr);
                        tst = false;
                        nowr = false;
                    }
                    strTableBody = strTableBody + "</tr>";
                    if (row["info"] != null & row["info"].ToString() != "")
                    {
                        if (haveshort)
                        {
                            strTableBody = strTableBody + "<tr> <td colspan=" + (Char)34 + spec_kol.Table.Columns.Count + (Char)34 + "><b>" + HTMLEncode(row["info"].ToString().Replace("::", ":")) + "</td></b></tr>";
                        }
                    }
                    if (haveshort)
                    {
                        Shortcut = Shortcut + "%26key" + ind + "%3D0%255E" + row["C_lin"] + "%255E" + row["cust_ord"] + "%255E" + row["c_rel"];
                    }
                    ind++;
                }
                Shortcut = Shortcut + "%26COMPANY%3DSITS" + (Char)34 + ">" + HTMLEncode("Pokaż linie zamówień") + "</a> </p>" + "<br />Dotyczy zam:" + txtwh.Substring(0, txtwh.Length - 1) + "<br /> ";
                strTableBody = firstbod + strTableBody + strTableEnd + Shortcut + lastbod;
                StrTableStart = StrTableStart + txtwh.Substring(0, txtwh.Length - 1);
                // Command line argument must the the SMTP host.
                SmtpClient client = new SmtpClient()
                {
                    Port = 587,
                    DeliveryFormat = SmtpDeliveryFormat.International,
                    Host = "host",
                    EnableSsl = false,
                    Timeout = 10000,
                    DeliveryMethod = SmtpDeliveryMethod.Network,
                    UseDefaultCredentials = false,
                    Credentials = new System.Net.NetworkCredential("email.adress", "qr67dfRR10fre")
                };
               
                MailMessage mm = new MailMessage("email.adress", mailto, StrTableStart, strTableBody)
                {
                    BodyTransferEncoding = System.Net.Mime.TransferEncoding.Base64,
                    SubjectEncoding = System.Text.Encoding.UTF8,
                    BodyEncoding = System.Text.Encoding.UTF8,
                    Priority = MailPriority.High,
                    HeadersEncoding = System.Text.Encoding.UTF8,
                    IsBodyHtml = true,
                };
                //mm.Bcc.Add(bcc);
                await client.SendMailExAsync(mm);
                mm.Dispose();
                client.Dispose();
                GC.Collect();
                Log("Wysłano maila:" + StrTableStart + txtwh + " Do:" + mailto);
                return 0;
            }
            catch (Exception e)
            {
                Log("Błąd w wysłaniu maila:" + StrTableStart + txtwh + " To:" + mailto + " errors" + e);
                return 1;
            }
        }
        private async Task<int> Prep_potw(DataRow[] rek)
        {
            using (DataTable kol = new DataTable())
            {
                using (NpgsqlConnection conA = new NpgsqlConnection(npA))
                {
                    await conA.OpenAsync();
                    using (NpgsqlCommand pot = new NpgsqlCommand("" +
                        "select CORR,CUST_ORD,C_LIN,C_REL,CATALOG_DESC,C_RY,PROM_WEEK,PROD_WEEK,PROD_DATE,PART_BUYER,SHORTAGE_PART,SHORT_NAM,DOP,CREATED " +
                        "from send_mail", conA))
                    {
                        using (NpgsqlDataReader po = pot.ExecuteReader())
                        {
                            using (DataTable sch = po.GetSchemaTable())
                            {
                                foreach (DataRow a in sch.Rows)
                                {
                                    kol.Columns.Add(a["ColumnName"].ToString().ToUpper(), System.Type.GetType("System.Int32"));
                                }
                            }
                        }
                    }
                }
                DataRow rw = kol.NewRow();
                for (int i = 0; i < kol.Columns.Count; i++)
                {
                    rw[i] = 0;
                }
                rw["PROD_WEEK"] = 1;
                rw["PROD_DATE"] = 1;
                kol.Rows.Add(rw);

                using (NpgsqlConnection conA = new NpgsqlConnection(npA))
                {
                    await conA.OpenAsync();
                    using (NpgsqlCommand cmd = new NpgsqlCommand("select CORR,CUST_ORD,C_LIN,C_REL,CATALOG_DESC,C_RY,PROM_WEEK,PROD_WEEK,PROD_DATE,PART_BUYER,SHORTAGE_PART,SHORT_NAM,DOP,CREATED,info from send_mail where mail=@mail and typ='MAIL' and is_confirm(status_informacji) is true and last_mail is null and created + interval '1 hour' < current_timestamp order by CORR,CUST_ORD,C_LIN,C_REL", conA))
                    {
                        cmd.Parameters.Add("mail", NpgsqlTypes.NpgsqlDbType.Varchar);
                        cmd.Prepare();
                        foreach (DataRow erw in rek)
                        {
                            cmd.Parameters[0].Value = erw[0];
                            using (NpgsqlDataReader re = cmd.ExecuteReader())
                            {
                                using (DataTable mal = new DataTable())
                                {
                                    mal.Load(re);
                                    if (mal.Rows.Count > 0)
                                    {
                                        int send = await Create_HTMLmail(mal, "Proszę o zmianę daty obiecanej", erw[0].ToString().Replace("\r", ""), kol.Rows[0], "*Powyższe linie zamówień zostały już przesunięte w produkcji na termin gwarantujący dostawę brakujących komponentów");
                                        if (send == 0)
                                        {
                                            using (NpgsqlCommand cmd1 = new NpgsqlCommand("UPDATE public.send_mail	SET last_mail=current_date WHERE  mail=@mail and typ='MAIL' and is_confirm(status_informacji) is true and last_mail is null and created + interval '1 hour' < current_timestamp", conA))
                                            {
                                                cmd1.Parameters.Add("mail", NpgsqlTypes.NpgsqlDbType.Varchar).Value = erw[0].ToString();
                                                cmd1.Prepare();
                                                cmd1.ExecuteNonQuery();
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            return 0;
        }
        private async Task<int> Prep_FR(DataRow[] rek)
        {
            using (DataTable kol = new DataTable())
            {
                using (NpgsqlConnection conA = new NpgsqlConnection(npA))
                {
                    await conA.OpenAsync();
                    using (NpgsqlCommand pot = new NpgsqlCommand("" +
                        "select CORR,CUST_ORD,C_LIN,C_REL,CATALOG_DESC,C_RY,PROM_WEEK,PROD_WEEK,PROD_DATE,PART_BUYER,SHORTAGE_PART,SHORT_NAM,DOP,CREATED " +
                        "from send_mail", conA))
                    {
                        using (NpgsqlDataReader po = pot.ExecuteReader())
                        {
                            using (DataTable sch = po.GetSchemaTable())
                            {
                                foreach (DataRow a in sch.Rows)
                                {
                                    kol.Columns.Add(a["ColumnName"].ToString().ToUpper(), System.Type.GetType("System.Int32"));
                                }
                            }
                        }
                    }
                    conA.Close();
                }
                DataRow rw = kol.NewRow();
                for (int i = 0; i < kol.Columns.Count; i++)
                {
                    rw[i] = 0;
                }
                rw["PROD_WEEK"] = 1;
                rw["PROD_DATE"] = 1;
                rw["SHORTAGE_PART"] = 1;
                rw["SHORT_NAM"] = 1;
                kol.Rows.Add(rw);
                using (NpgsqlConnection conA = new NpgsqlConnection(npA))
                {
                    await conA.OpenAsync();
                    using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                        "select CORR,CUST_ORD,C_LIN,C_REL,CATALOG_DESC,C_RY,PROM_WEEK,PROD_WEEK,PROD_DATE,PART_BUYER,SHORTAGE_PART,SHORT_NAM,DOP,CREATED,info " +
                        "from send_mail " +
                        "where mail=@mail and typ='MAIL' " +
                        "and is_alter(status_informacji) is true and last_mail is null and created + interval '1 hour' < current_timestamp " +
                        "order by CORR,CUST_ORD,C_LIN,C_REL", conA))
                    {
                        cmd.Parameters.Add("mail", NpgsqlTypes.NpgsqlDbType.Varchar);
                        cmd.Prepare();
                        foreach (DataRow erw in rek)
                        {
                            cmd.Parameters[0].Value = erw[0];
                            using (NpgsqlDataReader re = cmd.ExecuteReader())
                            {
                                using (DataTable mal = new DataTable())
                                {
                                    mal.Load(re);
                                    if (mal.Rows.Count > 0)
                                    {
                                        int send = await Create_HTMLmail(mal, "Proszę o zmianę daty obiecanej lub podmianę komponentu", erw[0].ToString().Replace("\r", ""), kol.Rows[0]);
                                        if (send == 0)
                                        {
                                            using (NpgsqlCommand cmd1 = new NpgsqlCommand("" +
                                                "UPDATE public.send_mail	" +
                                                "SET last_mail=current_date " +
                                                "WHERE  mail=@mail " +
                                                "and typ='MAIL' and is_alter(status_informacji) is true and last_mail is null and created + interval '1 hour' < current_timestamp", conA))
                                            {
                                                cmd1.Parameters.Add("mail", NpgsqlTypes.NpgsqlDbType.Varchar).Value = erw[0].ToString();
                                                cmd1.Prepare();
                                                cmd1.ExecuteNonQuery();
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            return 0;
        }
        private async Task<int> Prep_seriaz(DataRow[] rek)
        {
            using (DataTable kol = new DataTable())
            {
                using (NpgsqlConnection conA = new NpgsqlConnection(npA))
                {
                    await conA.OpenAsync();
                    using (NpgsqlCommand pot = new NpgsqlCommand("" +
                        "select CORR,CUST_ORD,C_LIN,C_REL,CATALOG_DESC,C_RY,PROM_WEEK,PROD_WEEK,PROD_DATE,TYP_ZDARZENIA,STATUS_INFORMACJI,PART_BUYER,SHORTAGE_PART,SHORT_NAM,DOP,CREATED " +
                        "from send_mail", conA))
                    {
                        using (NpgsqlDataReader po = pot.ExecuteReader())
                        {
                            using (DataTable sch = po.GetSchemaTable())
                            {
                                foreach (DataRow a in sch.Rows)
                                {
                                    kol.Columns.Add(a["ColumnName"].ToString().ToUpper(), System.Type.GetType("System.Int32"));
                                }
                            }
                        }
                    }
                    conA.Close();
                }
                DataRow rw = kol.NewRow();
                for (int i = 0; i < kol.Columns.Count; i++)
                {
                    rw[i] = 0;
                }
                rw["PROD_WEEK"] = 1;
                rw["PROD_DATE"] = 1;
                rw["SHORTAGE_PART"] = 1;
                rw["SHORT_NAM"] = 1;
                kol.Rows.Add(rw);
                using (NpgsqlConnection conA = new NpgsqlConnection(npA))
                {
                    await conA.OpenAsync();
                    using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                        "select CORR,CUST_ORD,C_LIN,C_REL,CATALOG_DESC,C_RY,PROM_WEEK,PROD_WEEK,PROD_DATE,TYP_ZDARZENIA,STATUS_INFORMACJI,PART_BUYER,SHORTAGE_PART,SHORT_NAM,DOP,CREATED,info " +
                        "from send_mail " +
                        "where mail=@mail and typ='Seria Zero' and last_mail is null " +
                        "and created + interval '1 hour' < current_timestamp order by CORR,CUST_ORD,C_LIN,C_REL", conA))
                    {
                        cmd.Parameters.Add("mail", NpgsqlTypes.NpgsqlDbType.Varchar);
                        cmd.Prepare();
                        foreach (DataRow erw in rek)
                        {
                            cmd.Parameters[0].Value = erw[0];
                            using (NpgsqlDataReader re = cmd.ExecuteReader())
                            {
                                using (DataTable mal = new DataTable())
                                {
                                    mal.Load(re);
                                    if (mal.Rows.Count > 0)
                                    {
                                        int send = await Create_HTMLmail(mal, "Proszę o zmianę daty obiecanej/zmianę daty wykonania SERII ZERO", erw[0].ToString().Replace("\r", ""), kol.Rows[0]);
                                        if (send == 0)
                                        {
                                            using (NpgsqlCommand cmd1 = new NpgsqlCommand("" +
                                                "UPDATE public.send_mail	" +
                                                "SET last_mail=current_date " +
                                                "WHERE  mail=@mail and typ='Seria Zero' and last_mail is null " +
                                                "and created + interval '1 hour' < current_timestamp", conA))
                                            {
                                                cmd1.Parameters.Add("mail", NpgsqlTypes.NpgsqlDbType.Varchar).Value = erw[0].ToString();
                                                cmd1.Prepare();
                                                cmd1.ExecuteNonQuery();
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            return 0;
        }
        private async Task<int> Prep_NIEzam(DataRow[] rek)
        {
            using (DataTable kol = new DataTable())
            {
                using (NpgsqlConnection conA = new NpgsqlConnection(npA))
                {
                    await conA.OpenAsync();
                    using (NpgsqlCommand pot = new NpgsqlCommand("" +
                        "select CORR,CUST_ORD,C_LIN,C_REL,CATALOG_DESC,C_RY,PROM_WEEK,PART_BUYER,SHORTAGE_PART,SHORT_NAM,DOP,CREATED " +
                        "from send_mail", conA))
                    {
                        using (NpgsqlDataReader po = pot.ExecuteReader())
                        {
                            using (DataTable sch = po.GetSchemaTable())
                            {
                                foreach (DataRow a in sch.Rows)
                                {
                                    kol.Columns.Add(a["ColumnName"].ToString().ToUpper(), System.Type.GetType("System.Int32"));
                                }
                            }
                        }
                    }
                    conA.Close();
                }
                DataRow rw = kol.NewRow();
                for (int i = 0; i < kol.Columns.Count; i++)
                {
                    rw[i] = 0;
                }
                rw["SHORTAGE_PART"] = 1;
                rw["SHORT_NAM"] = 1;
                kol.Rows.Add(rw);
                using (NpgsqlConnection conA = new NpgsqlConnection(npA))
                {
                    await conA.OpenAsync();
                    using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                        "select CORR,CUST_ORD,C_LIN,C_REL,CATALOG_DESC,C_RY,PROM_WEEK,PART_BUYER,SHORTAGE_PART,SHORT_NAM,DOP,CREATED,info " +
                        "from send_mail " +
                        "where mail=@mail and typ='MAIL' and is_dontpurch(status_informacji) is true " +
                        "and last_mail is null and created + interval '1 hour' < current_timestamp " +
                        "order by CORR,CUST_ORD,C_LIN,C_REL", conA))
                    {
                        cmd.Parameters.Add("mail", NpgsqlTypes.NpgsqlDbType.Varchar);
                        cmd.Prepare();
                        foreach (DataRow erw in rek)
                        {
                            cmd.Parameters[0].Value = erw[0];
                            using (NpgsqlDataReader re = cmd.ExecuteReader())
                            {
                                using (DataTable mal = new DataTable())
                                {
                                    mal.Load(re);
                                    if (mal.Rows.Count > 0)
                                    {
                                        int send = await Create_HTMLmail(mal, "Komponent nie zamawiany / wycofany ", erw[0].ToString().Replace("\r", ""), kol.Rows[0], "Produkcja powyższych zamówień jest zagrożona ze względu na użycie komponentów wycofanych z kolekcji / nie zamawianych");
                                        if (send == 0)
                                        {
                                            using (NpgsqlCommand cmd1 = new NpgsqlCommand("" +
                                                "UPDATE public.send_mail	" +
                                                "SET last_mail=current_date " +
                                                "WHERE  mail=@mail and typ='MAIL' and is_dontpurch(status_informacji) is true " +
                                                "and last_mail is null and created + interval '1 hour' < current_timestamp", conA))
                                            {
                                                cmd1.Parameters.Add("mail", NpgsqlTypes.NpgsqlDbType.Varchar).Value = erw[0].ToString();
                                                cmd1.Prepare();
                                                cmd1.ExecuteNonQuery();
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            return 0;
        }
        private async Task<int> Prep_NIEpotw(DataRow[] rek)
        {
            using (DataTable kol = new DataTable())
            {
                using (NpgsqlConnection conA = new NpgsqlConnection(npA))
                {
                    await conA.OpenAsync();
                    using (NpgsqlCommand pot = new NpgsqlCommand("" +
                        "select CORR,CUST_ORD,C_LIN,C_REL,CATALOG_DESC,C_RY,PROM_WEEK,PROD_WEEK,TYP_ZDARZENIA,STATUS_INFORMACJI,PART_BUYER,SHORTAGE_PART,SHORT_NAM,DOP,CREATED " +
                        "from send_mail", conA))
                    {
                        using (NpgsqlDataReader po = pot.ExecuteReader())
                        {
                            using (DataTable sch = po.GetSchemaTable())
                            {
                                foreach (DataRow a in sch.Rows)
                                {
                                    kol.Columns.Add(a["ColumnName"].ToString().ToUpper(), System.Type.GetType("System.Int32"));
                                }
                            }
                        }
                    }
                    conA.Close();
                }
                DataRow rw = kol.NewRow();
                for (int i = 0; i < kol.Columns.Count; i++)
                {
                    rw[i] = 0;
                }
                rw["SHORTAGE_PART"] = 1;
                rw["SHORT_NAM"] = 1;
                kol.Rows.Add(rw);
                using (NpgsqlConnection conA = new NpgsqlConnection(npA))
                {
                    await conA.OpenAsync();
                    using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                        "select CORR,CUST_ORD,C_LIN,C_REL,CATALOG_DESC,C_RY,PROM_WEEK,PROD_WEEK,TYP_ZDARZENIA,STATUS_INFORMACJI,PART_BUYER,SHORTAGE_PART,SHORT_NAM,DOP,CREATED,info " +
                        "from send_mail where mail=@mail and typ='NIE POTWIERDZAĆ' and last_mail is null and created + interval '1 hour' < current_timestamp order by CORR,CUST_ORD,C_LIN,C_REL", conA))
                    {
                        cmd.Parameters.Add("mail", NpgsqlTypes.NpgsqlDbType.Varchar);
                        cmd.Prepare();
                        foreach (DataRow erw in rek)
                        {
                            cmd.Parameters[0].Value = erw[0];
                            using (NpgsqlDataReader re = cmd.ExecuteReader())
                            {
                                using (DataTable mal = new DataTable())
                                {
                                    mal.Load(re);
                                    if (mal.Rows.Count > 0)
                                    {
                                        int send = await Create_HTMLmail(mal, "Brak możliwości automatycznego potwierdzenia zamówienia", erw[0].ToString().Replace("\r", ""), kol.Rows[0], "Produkcja powyższych zamówień jest zagrożona ze względu na status zamówienia,błędne daty obiecane,braki materiałowe,użycie komponentów wycofanych z kolekcji / nie zamawianych");
                                        if (send == 0)
                                        {
                                            using (NpgsqlCommand cmd1 = new NpgsqlCommand("" +
                                                "UPDATE public.send_mail	" +
                                                "SET last_mail=current_date " +
                                                "WHERE  mail=@mail and typ='NIE POTWIERDZAĆ' and last_mail is null " +
                                                "and created + interval '1 hour' < current_timestamp", conA))
                                            {
                                                cmd1.Parameters.Add("mail", NpgsqlTypes.NpgsqlDbType.Varchar).Value = erw[0].ToString();
                                                cmd1.Prepare();
                                                cmd1.ExecuteNonQuery();
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            return 0;
        }
        private async Task<int> Send_logist(DataRow[] rek)
        {
            using (DataTable kol = new DataTable())
            {
                using (NpgsqlConnection conA = new NpgsqlConnection(npA))
                {
                    await conA.OpenAsync();
                    using (NpgsqlCommand pot = new NpgsqlCommand("" +
                        "select CORR,CUST_ORD,C_LIN,C_REL,CATALOG_DESC,C_RY,LOAD_ID,SHIP_DATE,PROM_WEEK,PROD_WEEK,PROD_DATE as NEW_SHIP_DATE,PART_BUYER,SHORTAGE_PART,SHORT_NAM,DOP,CREATED " +
                        "from send_mail", conA))
                    {
                        using (NpgsqlDataReader po = pot.ExecuteReader())
                        {
                            using (DataTable sch = po.GetSchemaTable())
                            {
                                foreach (DataRow a in sch.Rows)
                                {
                                    kol.Columns.Add(a["ColumnName"].ToString().ToUpper(), System.Type.GetType("System.Int32"));
                                }
                            }
                        }
                    }
                    conA.Close();
                }
                DataRow rw = kol.NewRow();
                for (int i = 0; i < kol.Columns.Count; i++)
                {
                    rw[i] = 0;
                }
                rw["PROD_WEEK"] = 1;
                rw["SHIP_DATE"] = 1;
                rw["NEW_SHIP_DATE"] = 1;
                kol.Rows.Add(rw);

                using (NpgsqlConnection conA = new NpgsqlConnection(npA))
                {
                    await conA.OpenAsync();
                    using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                        "select CORR,CUST_ORD,C_LIN,C_REL,CATALOG_DESC,C_RY,LOAD_ID,SHIP_DATE,PROM_WEEK,PROD_WEEK,PROD_DATE as NEW_SHIP_DATE,PART_BUYER,SHORTAGE_PART,SHORT_NAM,DOP,CREATED,info " +
                        "from send_mail where mail=@mail and typ='MAIL LOG' and last_mail is null " +
                        "and created + interval '1 hour' < current_timestamp " +
                        "order by CORR,CUST_ORD,C_LIN,C_REL", conA))
                    {
                        cmd.Parameters.Add("mail", NpgsqlTypes.NpgsqlDbType.Varchar);
                        cmd.Prepare();
                        foreach (DataRow erw in rek)
                        {
                            cmd.Parameters[0].Value = erw[0];
                            using (NpgsqlDataReader re = cmd.ExecuteReader())
                            {
                                using (DataTable mal = new DataTable())
                                {
                                    mal.Load(re);
                                    if (mal.Rows.Count > 0)
                                    {
                                        int send = await Create_HTMLmail(mal, "Proszę o zmianę daty wysyłki", erw[0].ToString().Replace("\r", ""), kol.Rows[0], "*Powyższe linie zamówień zostały już przesunięte w produkcji na termin gwarantujący dostawę brakujących komponentów");
                                        if (send == 0)
                                        {
                                            using (NpgsqlCommand cmd1 = new NpgsqlCommand("" +
                                                "UPDATE public.send_mail	" +
                                                "SET last_mail=current_date " +
                                                "WHERE  mail=@mail and typ='MAIL LOG' and last_mail is null " +
                                                "and created + interval '1 hour' < current_timestamp", conA))
                                            {
                                                cmd1.Parameters.Add("mail", NpgsqlTypes.NpgsqlDbType.Varchar).Value = erw[0].ToString();
                                                cmd1.Prepare();
                                                cmd1.ExecuteNonQuery();
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            return 0;
        }
        private async Task<int> Popraw(DataRow[] rek)
        {
            using (DataTable kol = new DataTable())
            {
                using (NpgsqlConnection conA = new NpgsqlConnection(npA))
                {
                    await conA.OpenAsync();
                    using (NpgsqlCommand pot = new NpgsqlCommand("" +
                        "select CORR,CUST_ORD,C_LIN,C_REL,CATALOG_DESC,C_RY,PROM_WEEK,PART_BUYER,SHORTAGE_PART,SHORT_NAM,DOP,CREATED " +
                        "from send_mail", conA))
                    {
                        using (NpgsqlDataReader po = pot.ExecuteReader())
                        {
                            using (DataTable sch = po.GetSchemaTable())
                            {
                                foreach (DataRow a in sch.Rows)
                                {
                                    kol.Columns.Add(a["ColumnName"].ToString().ToUpper(), System.Type.GetType("System.Int32"));
                                }
                            }
                        }
                    }
                    conA.Close();
                }
                DataRow rw = kol.NewRow();
                for (int i = 0; i < kol.Columns.Count; i++)
                {
                    rw[i] = 0;
                }
                kol.Rows.Add(rw);

                using (NpgsqlConnection conA = new NpgsqlConnection(npA))
                {
                    await conA.OpenAsync();
                    using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                        "select CORR,CUST_ORD,C_LIN,C_REL,CATALOG_DESC,C_RY,PROM_WEEK,PART_BUYER,SHORTAGE_PART,SHORT_NAM,DOP,CREATED,info " +
                        "from send_mail " +
                        "where mail=@mail and typ='MAIL' and status_informacji='POPRAWIĆ' and last_mail is null " +
                        "and created + interval '1 hour' < current_timestamp " +
                        "order by CORR,CUST_ORD,C_LIN,C_REL", conA))
                    {
                        cmd.Parameters.Add("mail", NpgsqlTypes.NpgsqlDbType.Varchar);
                        cmd.Prepare();
                        foreach (DataRow erw in rek)
                        {
                            cmd.Parameters[0].Value = erw[0];
                            using (NpgsqlDataReader re = cmd.ExecuteReader())
                            {
                                using (DataTable mal = new DataTable())
                                {
                                    mal.Load(re);
                                    if (mal.Rows.Count > 0)
                                    {
                                        int send = await Create_HTMLmail(mal, "Proszę o poprawę dat obiecanych - problem z potwierdzeniem", erw[0].ToString().Replace("\r", ""), kol.Rows[0], "*Dla powyższych linii występuje problem z ustaleniem daty obiecanej - raport zprawdza linie z nieaktywnym DOP");
                                        if (send == 0)
                                        {
                                            using (NpgsqlCommand cmd1 = new NpgsqlCommand("" +
                                                "UPDATE public.send_mail	" +
                                                "SET last_mail=current_date " +
                                                "WHERE  mail=@mail and typ='MAIL' and status_informacji='POPRAWIĆ' " +
                                                "and created + interval '1 hour' < current_timestamp", conA))
                                            {
                                                cmd1.Parameters.Add("mail", NpgsqlTypes.NpgsqlDbType.Varchar).Value = erw[0].ToString();
                                                cmd1.Prepare();
                                                cmd1.ExecuteNonQuery();
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                    conA.Close();
                }
            }
            return 0;
        }
        private async Task<int> Send_mail_lack()
        {
            try
            {
                Log("Zaczynam przetwarzanie informacji o przepotwierdzeniach:" + (DateTime.Now - start));
                using (DataTable adres_list = new DataTable())
                {
                    using (NpgsqlConnection conA = new NpgsqlConnection(npA))
                    {
                        conA.Open();
                        using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                            "select cast(count(table_name) as integer) busy " +
                            "from public.datatbles " +
                            "where  substring(table_name,1,7)='cal_ord'  and in_progress=true", conA))
                        {
                            int busy_il = 1;
                            while (busy_il > 0)
                            {
                                busy_il = Convert.ToInt16(cmd.ExecuteScalar());
                            }
                        }
                        using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                            "UPDATE public.datatbles " +
                            "SET start_update=current_timestamp, in_progress=true,updt_errors=false " +
                            "WHERE table_name='TR_sendm'", conA))
                        {
                            cmd.ExecuteNonQuery();
                        }
                        using (NpgsqlTransaction TR_sendm = conA.BeginTransaction())
                        {
                            using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                                "UPDATE public.datatbles " +
                                "SET start_update=current_timestamp, in_progress=true,updt_errors=false " +
                                "WHERE table_name='send_mail'", conA))
                            {
                                cmd.ExecuteNonQuery();
                            }
                            using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                                "delete from send_mail where dop||'_'||mail||'_'||typ in (select a.dop||'_'||a.mail||'_'||a.typ " +
                                "from " +
                                    "(select *  from " +
                                    "send_mail except select * from fill_sendmail)a)", conA))
                            {
                                cmd.ExecuteNonQuery();
                            }
                            using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                                "INSERT INTO public.send_mail" +
                                "(mail, typ, typ_zdarzenia, status_informacji, info, corr, cust_ord, c_lin, c_rel, catalog_desc, c_ry,load_id,ship_date, prom_week, prod_week, prod_date, part_buyer, shortage_part, " +
                                "short_nam, dop, created, last_mail) " +
                                "select mail, typ, typ_zdarzenia, status_informacji, info, corr, cust_ord, c_lin, c_rel, catalog_desc, c_ry,load_id,ship_date, prom_week, prod_week, " +
                                "prod_date, part_buyer, shortage_part, short_nam, dop, created,null last_mail " +
                                    "from (select * from fill_sendmail except select * from send_mail)a", conA))
                            {
                                cmd.ExecuteNonQuery();
                            }
                            using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                                "UPDATE public.datatbles " +
                                "SET  last_modify=current_timestamp,in_progress=false,updt_errors=false " +
                                "WHERE table_name='TR_sendm'", conA))
                            {
                                cmd.ExecuteNonQuery();
                            }
                            TR_sendm.Commit();
                        }
                        using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                            "select cast(count(table_name) as integer) busy " +
                            "from public.datatbles " +
                            "where  table_name='TR_sendm' and in_progress=true", conA))
                        {
                            int busy_il = 1;
                            while (busy_il > 0)
                            {
                                busy_il = Convert.ToInt16(cmd.ExecuteScalar());
                                if (busy_il > 0) { System.Threading.Thread.Sleep(250); }
                            }
                        }
                        using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                            "select mail,typ,is_confirm(status_informacji) confirm, is_alter(status_informacji) alt,is_dontpurch(status_informacji) niezam," +
                            "case when typ!='NIE POTWIERDZAĆ' then status_informacji end tp " +
                            "from send_mail " +
                            "where last_mail is null and ((cast(created as date)<current_date and typ='NIE POTWIERDZAĆ') or (created + interval '1 hour' < current_timestamp and typ!='NIE POTWIERDZAĆ')) " +
                            "group by mail,is_confirm(status_informacji) ,is_alter(status_informacji) , is_dontpurch(status_informacji),typ,case when typ!='NIE POTWIERDZAĆ' then status_informacji end", conA))
                        {
                            using (NpgsqlDataReader re = cmd.ExecuteReader())
                            {
                                adres_list.Load(re);
                            }
                        }
                    }
                    int R_pot = 0;
                    int R_alter = 0;
                    int R_dontpurch = 0;
                    int dontconf = 0;
                    int seria_z = 0;
                    int log = 0;
                    int popr = 0;
                    int conirm = 0;
                    Parallel.Invoke(srv_op, async () => R_pot = await Prep_potw(adres_list.Select("confirm = true")), async () => R_alter = await Prep_FR(adres_list.Select("alt = true")), async () => R_dontpurch = await Prep_NIEzam(adres_list.Select("niezam = true")), async () => dontconf = await Prep_NIEpotw(adres_list.Select("typ = 'NIE POTWIERDZAĆ'")), async () => seria_z = await Prep_seriaz(adres_list.Select("typ = 'Seria Zero'")), async () => log = await Send_logist(adres_list.Select("typ = 'MAIL LOG'")), async () => popr = await Popraw(adres_list.Select("typ = 'MAIL' and tp='POPRAWIĆ'")), async () => conirm = await Confirm_ORd());
                    //Parallel.Invoke(async () => R_pot = await Prep_potw(adres_list.Select("confirm = true")), async () => R_alter = await Prep_FR(adres_list.Select("alt = true")), async () => R_dontpurch = await Prep_NIEzam(adres_list.Select("niezam = true")), async () => seria_z = await Prep_seriaz(adres_list.Select("typ = 'Seria Zero'")), async () => log = await Send_logist(adres_list.Select("typ = 'MAIL LOG'")), async () => popr = await Popraw(adres_list.Select("typ = 'MAIL' and tp='POPRAWIĆ'")), async () => conirm = await Confirm_ORd());
                }

                using (NpgsqlConnection conA = new NpgsqlConnection(npC))
                {
                    await conA.OpenAsync();
                    using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                        "UPDATE public.datatbles " +
                        "SET  last_modify=current_timestamp,in_progress=false,updt_errors=false " +
                        "WHERE table_name='send_mail'", conA))
                    {
                        cmd.ExecuteNonQuery();
                    }
                    conA.Close();
                }
                Log("Koniec wysyłania informacji o przepotwierdzeniach:" + (DateTime.Now - start));
                return 0;
            }
            catch (Exception e)
            {
                using (NpgsqlConnection conA = new NpgsqlConnection(npC))
                {
                    await conA.OpenAsync();
                    using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                        "UPDATE public.datatbles " +
                        "SET  last_modify=current_timestamp,in_progress=false,updt_errors=true " +
                        "WHERE table_name='send_mail'", conA))
                    {
                        cmd.ExecuteNonQuery();
                    }
                    conA.Close();
                }
                Log("Błąd w wysłaniu informacji o przepotwierdzeniach:" + e);
                return 1;
            }
        }
        private async Task<int> Capacity()
        {
            try
            {
                using (NpgsqlConnection conA = new NpgsqlConnection(npC))
                {
                    await conA.OpenAsync();
                    using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                        "UPDATE public.datatbles " +
                        "SET start_update=current_timestamp, in_progress=true,updt_errors=false " +
                        "WHERE table_name='CRP'", conA))
                    {
                        cmd.ExecuteNonQuery();
                    }
                }
                Log("START CRP " + (DateTime.Now - start));
                // Utwórz połączenie z ORACLE
                using (DataTable cap = new DataTable())
                {
                    using (OracleConnection conO = new OracleConnection("Password = pass;User ID = user; Data Source = prod8"))
                    {
                        await conO.OpenAsync();
                        using (OracleCommand crp = new OracleCommand("" +
                            "select To_Number(a.COUNTER||a.NOTE_ID) id, a.work_day,a.department_no,a.work_center_no,SUM(IFSAPP.Work_Center_Capacity_API.Get_Wc_Capac_Workday__('ST' , a.work_center_no, a.work_day)) capacity," +
                            "SUM(IFSAPP.Mach_Operation_Load_Util_API.Planned_Load(A.work_day,'ST',a.work_center_no)) planned,SUM(IFSAPP.Mach_Operation_Load_Util_API.Released_Load(A.work_day,'ST',a.work_center_no)) relased," +
                            "Nvl(Sum(c.godz),0) dop " +
                            "FROM " +
                                "(SELECT a.COUNTER,a.work_day,b.department_no,b.work_center_no,b.NOTE_ID " +
                                "FROM " +
                                "IFSAPP.work_time_calendar_pub A," +
                                "IFSAPP.work_center B " +
                                "WHERE A.work_day between SYSDATE-10 and SYSDATE+128 and A.calendar_id = IFSAPP.Work_Center_API.Get_Calendar_Id( B.contract, B.work_center_no ) and B.contract = 'ST' ) a " +
                                "left JOIN " +
                                "(SELECT  b.dat,ifsapp.work_center_api.Get_Department_No ('ST',b.WORK_CENTER_NO) wydz,b.WORK_CENTER_NO,Sum(b.godz) godz " +
                                "from " +
                                    "(SELECT DOP_ID " +
                                    "FROM ifsapp.dop_head " +
                                    "WHERE OBJSTATE IN ('Unreleased','Netted')) a ," +
                                    "(SELECT DOP_ID,DOP_ORDER_ID,WORK_CENTER_NO,ifsapp.dop_order_api.Get_Revised_Due_Date(DOP_ID,DOP_ORDER_ID) dat, Sum(MACH_RUN_FACTOR) godz " +
                                    "FROM ifsapp.dop_order_operation " +
                                    "WHERE ifsapp.dop_head_api.Get_Status(DOP_id) IN ('Unreleased','Netted') " +
                                    "GROUP BY  DOP_ID,DOP_ORDER_ID,WORK_CENTER_NO,ifsapp.dop_order_api.Get_Revised_Due_Date(DOP_ID,DOP_ORDER_ID)) b " +
                                "WHERE b.DOP_ID=a.DOP_ID GROUP BY b.dat,ifsapp.work_center_api.Get_Department_No ('ST',b.WORK_CENTER_NO),b.WORK_CENTER_NO ORDER BY dat,wydz,work_center_no) c  " +
                                "ON c.dat=a.work_day AND C.work_center_no=a.work_center_no " +
                                "group by a.COUNTER,a.work_day,a.department_no,a.work_center_no,a.NOTE_ID ORDER BY To_Number(a.COUNTER||a.NOTE_ID)", conO))
                        {
                            //mag.FetchSize = mag.FetchSize * 256;
                            using (OracleDataReader re = crp.ExecuteReader())
                            {
                                cap.Load(re);
                                cap.DefaultView.Sort = "id ASC";
                            }
                        }
                        using (DataTable cappstg = new DataTable())
                        {
                            using (NpgsqlConnection conA = new NpgsqlConnection(npA))
                            {
                                await conA.OpenAsync();
                                using (NpgsqlCommand pot = new NpgsqlCommand("Select * from \"CRP\" order by id", conA))
                                {
                                    using (NpgsqlDataReader po = pot.ExecuteReader())
                                    {
                                        using (DataTable sch = po.GetSchemaTable())
                                        {
                                            foreach (DataRow a in sch.Rows)
                                            {
                                                cappstg.Columns.Add(a["ColumnName"].ToString(), System.Type.GetType(a["DataType"].ToString()));
                                                cappstg.Columns[a["ColumnName"].ToString()].AllowDBNull = true;
                                            }
                                        }
                                        cappstg.Load(po);
                                    }
                                }
                            }
                            using (DataTable tmp_cap = cappstg.Clone())
                            {
                                tmp_cap.Columns.Add("stat");
                                int maxrek = cappstg.Rows.Count;
                                int ind = 0;
                                int counter = -1;
                                int max = cap.Rows.Count;
                                try
                                {
                                    foreach (DataRowView rek in cap.DefaultView)
                                    {
                                        if (counter < max) { counter++; }
                                        if (maxrek > ind)
                                        {
                                            while (Convert.ToInt64(rek["id"]) > Convert.ToInt64(cappstg.DefaultView[ind].Row["id"]))
                                            {
                                                //Debug.Print("DEL " + mgpstg.DefaultView[ind].Row[0]);
                                                DataRow rw = tmp_cap.NewRow();
                                                for (int i = 0; i < tmp_cap.Columns.Count - 1; i++)
                                                {
                                                    rw[i] = cappstg.DefaultView[ind].Row[i] ?? DBNull.Value;
                                                }
                                                rw[tmp_cap.Columns.Count - 1] = "DEL";
                                                tmp_cap.Rows.Add(rw);
                                                ind++;
                                                if (maxrek <= ind) { break; }
                                            }
                                            if (maxrek > ind)
                                            {
                                                if (Convert.ToInt64(rek["id"]) == Convert.ToInt64(cappstg.DefaultView[ind].Row["id"]))
                                                {
                                                    //Debug.Print("Compare " + rek["note_id"]);
                                                    bool chk = false;
                                                    for (int i = 1; i < tmp_cap.Columns.Count - 1; i++)
                                                    {
                                                        if (i > 3)
                                                        {
                                                            if (Convert.ToDouble(rek[i]) != Convert.ToDouble(cappstg.DefaultView[ind].Row[i]))
                                                            {
                                                                chk = true;
                                                            }
                                                        }
                                                        else
                                                        {
                                                            if (rek[i].ToString() != cappstg.DefaultView[ind].Row[i].ToString())
                                                            {
                                                                chk = true;
                                                            }

                                                        }
                                                    }
                                                    if (chk)
                                                    {
                                                        //Debug.Print("MOD " + rek["note_id"]);
                                                        DataRow rw = tmp_cap.NewRow();
                                                        for (int i = 0; i < tmp_cap.Columns.Count - 1; i++)
                                                        {
                                                            rw[i] = rek[i] ?? DBNull.Value;
                                                        }
                                                        rw[tmp_cap.Columns.Count - 1] = "MOD";
                                                        tmp_cap.Rows.Add(rw);
                                                    }
                                                    ind++;
                                                }
                                                else
                                                {
                                                    //Log("ADD " + rek["note_id"]);
                                                    DataRow rw = tmp_cap.NewRow();
                                                    for (int i = 0; i < tmp_cap.Columns.Count - 1; i++)
                                                    {
                                                        rw[i] = rek[i];
                                                    }
                                                    rw[tmp_cap.Columns.Count - 1] = "ADD";
                                                    tmp_cap.Rows.Add(rw);
                                                }
                                            }
                                            else
                                            {
                                                DataRow rw = tmp_cap.NewRow();
                                                for (int i = 0; i < tmp_cap.Columns.Count - 1; i++)
                                                {
                                                    rw[i] = rek[i] ?? DBNull.Value;
                                                }
                                                rw[tmp_cap.Columns.Count - 1] = "ADD";
                                                tmp_cap.Rows.Add(rw);
                                            }
                                            if (counter >= max)
                                            {
                                                while (maxrek > ind)
                                                {
                                                    // Log("DEL " + mgpstg.DefaultView[ind].Row[0]);
                                                    DataRow rw = tmp_cap.NewRow();
                                                    for (int i = 0; i < tmp_cap.Columns.Count - 1; i++)
                                                    {
                                                        rw[i] = cappstg.DefaultView[ind].Row[i] ?? DBNull.Value;
                                                    }
                                                    rw[tmp_cap.Columns.Count - 1] = "DEL";
                                                    tmp_cap.Rows.Add(rw);
                                                    ind++;
                                                    if (maxrek <= ind) { break; }
                                                }
                                            }
                                        }
                                        else
                                        {
                                            //Debug.Print("ADD " + rek["note_id"]);
                                            DataRow rw = tmp_cap.NewRow();
                                            for (int i = 0; i < tmp_cap.Columns.Count - 1; i++)
                                            {
                                                rw[i] = rek[i] ?? DBNull.Value;
                                            }
                                            rw[tmp_cap.Columns.Count - 1] = "ADD";
                                            tmp_cap.Rows.Add(rw);
                                        }
                                    }
                                }
                                catch (Exception e)
                                {
                                    using (NpgsqlConnection conB = new NpgsqlConnection(npC))
                                    {
                                        await conB.OpenAsync();
                                        using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                                            "UPDATE public.datatbles " +
                                            "SET in_progress=false,updt_errors=true " +
                                            "WHERE table_name='CRP'", conB))
                                        {
                                            cmd.ExecuteNonQuery();
                                        }
                                    }
                                    Log("Błąd aktualizacji stanów CRP:" + e);
                                    return 1;
                                }
                                try
                                {
                                    using (NpgsqlConnection conA = new NpgsqlConnection(npA))
                                    {
                                        conA.Open();
                                        //Start update
                                        using (NpgsqlTransaction TR_crp = conA.BeginTransaction())
                                        {
                                            DataRow[] rwA = tmp_cap.Select("stat='MOD'");
                                            Log("RECORDS invent mod: " + rwA.Length);
                                            if (rwA.Length > 0)
                                            {
                                                using (NpgsqlCommand cmd2 = new NpgsqlCommand("" +
                                                    "UPDATE public.\"CRP\" " +
                                                    "SET work_day=@work_day, department_no=@department_no, work_center_no=@work_center_no, capacity=@capacity, planned=@planned, relased=@relased,\"DOP\"=@dop  " +
                                                    "where \"id\"=@id", conA))
                                                {
                                                    cmd2.Parameters.Add("id", NpgsqlTypes.NpgsqlDbType.Bigint);
                                                    cmd2.Parameters.Add("work_day", NpgsqlTypes.NpgsqlDbType.Date);
                                                    cmd2.Parameters.Add("department_no", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                    cmd2.Parameters.Add("work_center_no", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                    cmd2.Parameters.Add("capacity", NpgsqlTypes.NpgsqlDbType.Double);
                                                    cmd2.Parameters.Add("planned", NpgsqlTypes.NpgsqlDbType.Double);
                                                    cmd2.Parameters.Add("relased", NpgsqlTypes.NpgsqlDbType.Double);
                                                    cmd2.Parameters.Add("dop", NpgsqlTypes.NpgsqlDbType.Double);
                                                    cmd2.Prepare();
                                                    foreach (DataRow rA in rwA)
                                                    {
                                                        cmd2.Parameters["id"].Value = Convert.ToInt64(rA["id"]);
                                                        cmd2.Parameters["work_day"].Value = (DateTime)rA["work_day"];
                                                        cmd2.Parameters["department_no"].Value = (string)rA["department_no"];
                                                        cmd2.Parameters["work_center_no"].Value = (string)rA["work_center_no"];
                                                        cmd2.Parameters["capacity"].Value = Convert.ToDouble(rA["capacity"]);
                                                        cmd2.Parameters["planned"].Value = Convert.ToDouble(rA["planned"]);
                                                        cmd2.Parameters["relased"].Value = Convert.ToDouble(rA["relased"]);
                                                        cmd2.Parameters["dop"].Value = Convert.ToDouble(rA["dop"]);
                                                        cmd2.ExecuteNonQuery();
                                                    }
                                                    Log("END MODIFY CRP for part:" + (DateTime.Now - start));
                                                }
                                            }
                                            // DODAJ dane
                                            rwA = tmp_cap.Select("stat='ADD'");
                                            Log("RECORDS mag add: " + rwA.Length);
                                            if (rwA.Length > 0)
                                            {
                                                using (NpgsqlCommand cmd2 = new NpgsqlCommand("" +
                                                    "INSERT INTO public.\"CRP\" " +
                                                    "(id,work_day,department_no,work_center_no,capacity,planned,relased,\"DOP\") " +
                                                    "VALUES " +
                                                    "(@id,@work_day,@department_no,@work_center_no,@capacity,@planned,@relased,@dop);", conA))
                                                {
                                                    cmd2.Parameters.Add("id", NpgsqlTypes.NpgsqlDbType.Bigint);
                                                    cmd2.Parameters.Add("work_day", NpgsqlTypes.NpgsqlDbType.Date);
                                                    cmd2.Parameters.Add("department_no", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                    cmd2.Parameters.Add("work_center_no", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                    cmd2.Parameters.Add("capacity", NpgsqlTypes.NpgsqlDbType.Double);
                                                    cmd2.Parameters.Add("planned", NpgsqlTypes.NpgsqlDbType.Double);
                                                    cmd2.Parameters.Add("relased", NpgsqlTypes.NpgsqlDbType.Double);
                                                    cmd2.Parameters.Add("dop", NpgsqlTypes.NpgsqlDbType.Double);
                                                    cmd2.Prepare();
                                                    foreach (DataRow rA in rwA)
                                                    {
                                                        cmd2.Parameters["id"].Value = Convert.ToInt64(rA["id"]);
                                                        cmd2.Parameters["work_day"].Value = (DateTime)rA["work_day"];
                                                        cmd2.Parameters["department_no"].Value = (string)rA["department_no"];
                                                        cmd2.Parameters["work_center_no"].Value = (string)rA["work_center_no"];
                                                        cmd2.Parameters["capacity"].Value = Convert.ToDouble(rA["capacity"]);
                                                        cmd2.Parameters["planned"].Value = Convert.ToDouble(rA["planned"]);
                                                        cmd2.Parameters["relased"].Value = Convert.ToDouble(rA["relased"]);
                                                        cmd2.Parameters["dop"].Value = Convert.ToDouble(rA["dop"]);
                                                        cmd2.ExecuteNonQuery();
                                                    }
                                                    Log("END ADD CRP for part:" + (DateTime.Now - start));
                                                }
                                            }
                                            rwA = tmp_cap.Select("stat='DEL'");
                                            if (rwA.Length > 0)
                                            {
                                                using (NpgsqlCommand cmd2 = new NpgsqlCommand("" +
                                                    "delete " +
                                                    "from public.\"CRP\" " +
                                                    "where id=@id", conA))
                                                {
                                                    cmd2.Parameters.Add("@id", NpgsqlTypes.NpgsqlDbType.Bigint);
                                                    cmd2.Prepare();
                                                    foreach (DataRow rA in rwA)
                                                    {
                                                        cmd2.Parameters[0].Value = rA["id"];
                                                        cmd2.ExecuteNonQuery();
                                                    }
                                                    Log("ERASE dont match CRP for part" + (DateTime.Now - start));
                                                }
                                            }
                                            using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                                                "UPDATE public.datatbles " +
                                                "SET last_modify=current_timestamp, in_progress=false " +
                                                "WHERE table_name='CRP'", conA))
                                            {
                                                cmd.ExecuteNonQuery();
                                            }
                                            TR_crp.Commit();
                                        }
                                    }
                                }
                                catch (Exception e)
                                {
                                    using (NpgsqlConnection conA = new NpgsqlConnection(npC))
                                    {
                                        await conA.OpenAsync();
                                        using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                                            "UPDATE public.datatbles " +
                                            "SET in_progress=false,updt_errors=true " +
                                            "WHERE table_name='CRP'", conA))
                                        {
                                            cmd.ExecuteNonQuery();
                                        }
                                    }
                                    Log("Błąd aktualizacji CRP w POSTGRSQL:" + e);
                                    return 1;
                                }
                            }
                        }
                    }
                }
                Log("READY CRP " + (DateTime.Now - start));
                return 0;
            }
            catch (Exception e)
            {
                using (NpgsqlConnection conA = new NpgsqlConnection(npC))
                {
                    await conA.OpenAsync();
                    using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                        "UPDATE public.datatbles " +
                        "SET in_progress=false,updt_errors=true " +
                        "WHERE table_name='CRP'", conA))
                    {
                        cmd.ExecuteNonQuery();
                    }
                }
                Log("Błąd aktualizacji CRP w POSTGRSQL:" + e);
                return 1;
            }
        }
        #endregion
        #region Funkcje słownika danych npdsql data types
        NpgsqlTypes.NpgsqlDbType GetOleDbType(System.Type typ)
        {
            try
            {
                return typeMap[(typ)];
            }
            catch
            {
                return NpgsqlTypes.NpgsqlDbType.Varchar;
            }
        }
        string GE_LEN_DATA(string coNam)
        {
            switch (coNam.ToUpper())
            {
                case "INDEKS":
                    { return "(15)"; }
                case "OPIS":
                    { return "(150)"; }
                case "KOLEKCJA":
                    { return "(40)"; }
                case "PLANNER_BUYER":
                    { return "(6)"; }
                case "RODZAJ":
                    { return "(15)"; }
                case "STATUS_INFORMACJI":
                    { return "(35)"; }
                case "PRZYCZYNA":
                    { return "(25)"; }
                case "TYP_ZDARZENIA":
                    { return "(35)"; }
                default:
                    { return ""; }
            }
        }
        #endregion
    }  
}


