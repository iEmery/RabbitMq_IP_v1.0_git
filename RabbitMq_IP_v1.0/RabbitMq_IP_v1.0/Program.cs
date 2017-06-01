using Bukimedia.PrestaSharp.Factories;
using Messages_ClassLibrary;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RabbitMq_IP_v1._0
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("RabbitMq_IP_v1.0 strarted...");

            //API's
            string HMS_CRM_BASE_URL = "http://10.3.51.37:32200/api";
            string HMS_CRM_ACCOUNT = "HAK8IHRW4KWXTQS8CAIJ7YESQDUNYCPX";
            string HMS_CRM_PASSWORD = "";

            string HMS_NINJA_BASE_URL = "http://10.3.51.37:32200/api";
            string HMS_NINJA_ACCOUNT = "WCD4783XZL63IAB6G8IUZ8NZLWBPRY7S";
            string HMS_NINJA_PASSWORD = "";

            string POS_BASE_URL = "http://10.3.51.37:32700/api";
            string POS_ACCOUNT = "BS9B3BWGCY5MXQ8KWDUUIR7TJP3RW3LF";
            string POS_PASSWORD = "";

            string CRM_BASE_URL = "";
            string CRM_ACCOUNT = "User";
            string CRM_PASSWORD = "6a4dc9133d5f3b6d9fff778aff361961";

            Task hms_crm_sender = new Task(RabbitMq_IP.Notify_new_hms_customers(HMS_CRM_BASE_URL, HMS_CRM_ACCOUNT, HMS_CRM_PASSWORD, "HMS"));
            Task hms_ninja_sender = new Task(RabbitMq_IP.Notify_new_orders(HMS_NINJA_BASE_URL, HMS_NINJA_ACCOUNT, HMS_NINJA_PASSWORD, "HMS"));
            Task pos_sender = new Task(RabbitMq_IP.Notify_new_orders(POS_BASE_URL, POS_ACCOUNT, POS_PASSWORD, "POS"));
            Task crm_sender = new Task(RabbitMq_IP.Notify_new_crm_customers(CRM_BASE_URL, CRM_ACCOUNT, CRM_PASSWORD, "CRM"));

            Task hms_crm_receiver = new Task(RabbitMq_IP.New_hms_customer_notified());
            Task crm_pos_receiver = new Task(RabbitMq_IP.New_pos_customer_notified(POS_BASE_URL, POS_ACCOUNT, POS_PASSWORD));

            Console.WriteLine("Senders started...");
            //RABBITMQ SENDERS
            hms_crm_sender.Start();
            hms_ninja_sender.Start();
            //pos_sender.Start();
            //pos_sender.Wait();
            crm_sender.Start();


            Console.WriteLine("Receivers started...\n");
            //RABBITMQ RECEIVERS
            hms_crm_receiver.Start();
            hms_crm_receiver.Wait();
            crm_pos_receiver.Start();
            crm_pos_receiver.Wait();

        }

        
    }

}
