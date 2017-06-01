using Bukimedia.PrestaSharp.Factories;
using Messages_ClassLibrary.ServiceReference1;
using Newtonsoft.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Messages_ClassLibrary
{
    public static class RabbitMq_IP
    {

        //NEW ORDER SENDER
        public static Action Notify_new_orders(
           string BASE_URL, string ACCOUNT, string PASSWORD, string SENDER)
        {
            return new Action(() => {
                ///

                Console.WriteLine("New " + SENDER + " orders notifier strarted. @Thread: " + Thread.CurrentThread.ManagedThreadId);
                
                //CHECK NEW ORDERS
                List<Bukimedia.PrestaSharp.Entities.order> actual_list = new List<Bukimedia.PrestaSharp.Entities.order>();
                while (true)
                {
                    OrderFactory orders = new OrderFactory(BASE_URL, ACCOUNT, PASSWORD);
                    List<Bukimedia.PrestaSharp.Entities.order> all_presta_orders = orders.GetAll();

                    IEnumerable<Bukimedia.PrestaSharp.Entities.order> check_list = new List<Bukimedia.PrestaSharp.Entities.order>();
                    if (actual_list.Count == 0)
                    {
                        Console.WriteLine("Initialize message will be sendend");
                        check_list = all_presta_orders;
                    }
                    else
                    {
                        //CHECK IF NEW ORDERS ADDED
                        foreach (Bukimedia.PrestaSharp.Entities.order o in all_presta_orders)
                        {
                            bool contain = false;
                            foreach (Bukimedia.PrestaSharp.Entities.order o1 in actual_list)
                            {
                                if (o.id == o1.id)
                                {
                                    contain = true;
                                    break;
                                }
                            }

                            if (contain == false)
                            {
                                List<Bukimedia.PrestaSharp.Entities.order> new_check_list = check_list.ToList<Bukimedia.PrestaSharp.Entities.order>();
                                new_check_list.Add(o);
                                check_list = new_check_list.ToList();
                            }

                        }
                    }


                    //CREATE NEW MESSAGES FROM NEW DATA
                    Message_new_order[] messages = new Message_new_order[check_list.Count()];
                    if (check_list.Count() != 0 || actual_list.Count() == 0)
                    {

                        int index1 = 0;
                        foreach (Bukimedia.PrestaSharp.Entities.order order in check_list)
                        {
                            Bukimedia.PrestaSharp.Entities.customer customer = new CustomerFactory(BASE_URL, ACCOUNT, PASSWORD).Get((long)order.id_customer);
                            Bukimedia.PrestaSharp.Entities.address customer_address = new AddressFactory(BASE_URL, ACCOUNT, PASSWORD).Get((long)order.id_address_invoice);
                            Message_new_order.Order.Product[] order_products = new Message_new_order.Order.Product[order.associations.order_rows.Count];
                            Message_new_order.Order[] order_orders = new Message_new_order.Order[1];


                            int index2 = 0;
                            foreach (Bukimedia.PrestaSharp.Entities.order_row row in order.associations.order_rows)
                            {
                                Bukimedia.PrestaSharp.Entities.product product = new ProductFactory(BASE_URL, ACCOUNT, PASSWORD).Get((long)row.product_id);
                                Message_new_order.Order.Product p = new Message_new_order.Order.Product()
                                {
                                    Name = product.name.First().Value,
                                    Description = product.description.First().Value,
                                    Price = product.price,
                                    Price_tax_incl = row.unit_price_tax_incl,
                                    Quantity = row.product_quantity
                                };
                                order_products[index2] = p;

                                index2++;
                            }

                            Message_new_order.Order o = new Message_new_order.Order()
                            {
                                Date_invoice = order.invoice_date,
                                Payment_method = order.payment,
                                Discount = order.total_discounts_tax_excl,
                                Shipping = order.total_shipping_tax_excl,
                                Total = order.total_paid_tax_excl,
                                Discount_taxt_incl = order.total_discounts_tax_incl,
                                Shipping_tax_incl = order.total_discounts_tax_incl,
                                Total_tax_incl = order.total_paid_tax_incl,
                                Producten = order_products
                            };
                            order_orders[0] = o;



                            Message_new_order message = new Message_new_order()
                            {
                                Sender = SENDER,
                                Firstname = customer.firstname,
                                Lastname = customer.lastname,
                                Email = customer.email,
                                Birthday = customer.birthday,
                                Address1 = customer_address.address1,
                                Address2 = customer_address.address2,
                                Postcode = customer_address.postcode,
                                City = customer_address.city,
                                Phone = customer_address.phone,
                                Phone_mobile = customer_address.phone_mobile,
                                Orders = order_orders

                            };

                            messages[index1] = message;
                            index1++;
                        }

                    }

                    //SEND NEW DATA
                    if (messages.Count() != 0)
                    {
                        //RabbitMq
                        string R_EXCHANGE = "new_order_exchange";
                        string R_QUEUE = "new_order_queue";
                        string R_KEY = "new_order_queue";

                        ConnectionFactory factory = new ConnectionFactory();
                        factory.UserName = "guest";
                        factory.Password = "guest";
                        factory.HostName = "10.3.51.37";

                        IConnection conn = factory.CreateConnection();
                        IModel channel = conn.CreateModel();

                        channel.ExchangeDeclare(
                            exchange: R_EXCHANGE,
                            type: ExchangeType.Direct);
                        channel.QueueDeclare(
                            queue: R_QUEUE,
                            durable: true,
                            exclusive: false,
                            autoDelete: false,
                            arguments: null);
                        channel.QueueBind(
                            queue: R_QUEUE,
                            exchange: R_EXCHANGE,
                            routingKey: R_KEY);

                        string messageBody = JsonConvert.SerializeObject(messages);
                        byte[] messageBodyBytes = Encoding.UTF8.GetBytes(messageBody);

                        //Send
                        channel.BasicPublish(
                            exchange: R_EXCHANGE,
                            routingKey: R_KEY,
                            basicProperties: null,
                            body: messageBodyBytes);

                        Console.WriteLine("new data sended to RabbitMq. @Thread: " + Thread.CurrentThread.ManagedThreadId);
                        channel.Dispose();
                        conn.Dispose();
                    }

                    actual_list = all_presta_orders;
                }


                ///
            });
        }
        

        //NEW HMS CUSTOMER SENDER
        public static Action Notify_new_hms_customers(
          string BASE_URL, string ACCOUNT, string PASSWORD, string SENDER)
        {
            return new Action(() => {
                ///

                Console.WriteLine("!New HMS customers notifier strarted. @Thread: " + Thread.CurrentThread.ManagedThreadId + "\n");

                //Notify new customers
                List<Bukimedia.PrestaSharp.Entities.customer> actual_list = new List<Bukimedia.PrestaSharp.Entities.customer>();
                while (true)
                {
                    //Get all users from HMS
                    CustomerFactory customers = new CustomerFactory(BASE_URL, ACCOUNT, PASSWORD);
                    List<Bukimedia.PrestaSharp.Entities.customer> all_presta_customers = customers.GetAll();

                    //Check for new data
                    IEnumerable<Bukimedia.PrestaSharp.Entities.customer> check_list = new List<Bukimedia.PrestaSharp.Entities.customer>();
                    if (actual_list.Count == 0)
                    {
                        Console.WriteLine("Initialize message will be sendend");
                        check_list = all_presta_customers;
                    }
                    else
                    {
                        foreach (Bukimedia.PrestaSharp.Entities.customer c in all_presta_customers)
                        {
                            bool contain = false;
                            foreach (Bukimedia.PrestaSharp.Entities.customer c1 in actual_list)
                            {
                                if (c.id == c1.id)
                                {
                                    contain = true;
                                    break;
                                }
                            }
                            if (contain == false)
                            {
                                List<Bukimedia.PrestaSharp.Entities.customer> tmp1 = check_list.ToList<Bukimedia.PrestaSharp.Entities.customer>();
                                tmp1.Add(c);
                                check_list = tmp1.ToList();
                            }

                        }
                    }


                    //Format messages from new data
                    Messages_ClassLibrary.Message_new_customer[] messages = new Messages_ClassLibrary.Message_new_customer[check_list.Count()];
                    if (check_list.Count() != 0 || actual_list.Count() == 0)
                    {

                        Console.WriteLine("New customers count: " + check_list.Count());
                        int index = 0;
                        foreach (Bukimedia.PrestaSharp.Entities.customer c in check_list)
                        {

                            //Message
                            Bukimedia.PrestaSharp.Entities.address customer_address = getAdres((long)c.id, BASE_URL, ACCOUNT, PASSWORD);
                            Messages_ClassLibrary.Message_new_customer message = new Messages_ClassLibrary.Message_new_customer()
                            {
                                firstname = c.firstname,
                                lastname = c.lastname,
                                email = c.email,
                                birthday = c.birthday,
                                address1 = (customer_address != null && customer_address.address1 != null) ? customer_address.address1.ToString() : "",
                                address2 = (customer_address != null && customer_address.address2 != null) ? customer_address.address2.ToString() : "",
                                postcode = (customer_address != null && customer_address.postcode != null) ? customer_address.postcode.ToString() : "",
                                city = (customer_address != null && customer_address.city != null) ? customer_address.city.ToString() : "",
                                phone = (customer_address != null && customer_address.phone != null) ? customer_address.phone.ToString() : "",
                                phone_mobile = (customer_address != null && customer_address.phone_mobile != null) ? customer_address.phone_mobile.ToString() : "",
                            };
                            messages[index] = message;
                            index++;

                        }

                    }

                    //Send new data
                    if (messages.Count() != 0)
                    {
                        //RabbitMq
                        ConnectionFactory factory = new ConnectionFactory();
                        factory.UserName = "guest";
                        factory.Password = "guest";
                        factory.HostName = "10.3.51.37";

                        IConnection conn = factory.CreateConnection();
                        IModel channel = conn.CreateModel();

                        channel.ExchangeDeclare(
                            exchange: "new_hms_customer_exchange",
                            type: ExchangeType.Direct);
                        channel.QueueDeclare(
                            queue: "hms_crm_customer_queue",
                            durable: true,
                            exclusive: false,
                            autoDelete: false,
                            arguments: null);
                        channel.QueueBind(
                            queue: "hms_crm_customer_queue",
                            exchange: "new_hms_customer_exchange",
                            routingKey: "new_hms_customer_exchange");

                        string messageBody = JsonConvert.SerializeObject(messages);
                        byte[] messageBodyBytes = Encoding.UTF8.GetBytes(messageBody);

                        //Send
                        channel.BasicPublish(
                            exchange: "new_hms_customer_exchange",
                            routingKey: "new_hms_customer_exchange",
                            basicProperties: null,
                            body: messageBodyBytes);

                        Console.WriteLine("new data sended. @Thread: " + Thread.CurrentThread.ManagedThreadId);
                        channel.Dispose();
                        conn.Dispose();
                    }

                    actual_list = all_presta_customers;

                }


                ///
            });
        }


        //NEW CRM CUSTOMER SENDER
        public static Action Notify_new_crm_customers(
          string BASE_URL, string ACCOUNT, string PASSWORD, string SENDER)
        {
            return new Action(() => {
                ///

                Console.WriteLine("!New CRM customers notifier strarted. @Thread: " + Thread.CurrentThread.ManagedThreadId + "\n");

                //Notify new customers
                string sugarCrmUsername = ACCOUNT;
                string sugarCrmPassword = PASSWORD;

                ServiceReference1.sugarsoapPortTypeClient soap = new ServiceReference1.sugarsoapPortTypeClient("sugarsoapPort");
                ServiceReference1.user_auth user = new ServiceReference1.user_auth();

                user.user_name = sugarCrmUsername;
                user.password = sugarCrmPassword;

                ServiceReference1.name_value[] loginList = new ServiceReference1.name_value[0];
                ServiceReference1.entry_value result_login = soap.login(user, "SOAP_RABBITMQ", loginList);

                string sessionId = result_login.id;

                ServiceReference1.get_entry_list_result_version2 actual_list = new get_entry_list_result_version2();


                while (true)
                {
                    //Get all users from CRM
                    ServiceReference1.get_entry_list_result_version2 suiteCrm_all_accounts = soap.get_entry_list(
                        sessionId,
                        "Accounts",
                        "",
                        "",
                        0,
                        new[] {
                            "id",
                            "name" ,
                            "email",
                            "billing_address_street",
                            "billing_address_street2",
                            "billing_address_city",
                            "billing_address_postalcode",
                            "phone_fax",
                            "phone_office" },
                        null,
                        99999,
                        0,
                        false);

                    ServiceReference1.get_entry_list_result_version2 check_list = new get_entry_list_result_version2();

                    if (actual_list.total_count == 0)
                    {
                        Console.WriteLine("Initialize message will be sendend");
                        check_list = suiteCrm_all_accounts;
                    }
                    else
                    {
                        foreach (entry_value c in suiteCrm_all_accounts.entry_list)
                        {
                            bool contain = false;
                            foreach (entry_value c1 in actual_list.entry_list)
                            {
                                if (c.id == c1.id)
                                {
                                    contain = true;
                                    break;
                                }
                            }
                            if (contain == false)
                            {
                                List<entry_value> tmp;
                                tmp = (check_list.entry_list == null) ? new List<entry_value>() : check_list.entry_list.ToList();
                                //tmp = check_list.entry_list.ToList();
                                tmp.Add(c);
                                check_list.entry_list = tmp.ToArray();
                            }

                        }
                    }


                    //Format messages from new data
                    Messages_ClassLibrary.Message_new_customer[] messages = new Messages_ClassLibrary.Message_new_customer[0];
                    if ( (check_list.entry_list != null && check_list.entry_list.Count() != 0) || actual_list.entry_list.Count() == 0)
                    {
                        messages = new Messages_ClassLibrary.Message_new_customer[check_list.entry_list.Count()];
                        Console.WriteLine("New customers count: " + check_list.entry_list.Count());

                        int index = 0;
                        foreach (entry_value c in check_list.entry_list)
                        {

                            string name =  GetValueFromNameValueList("name", c.name_value_list);
                            string firstname = name;
                            string lastname = name;

                            var names = name.Split(' ');
                            if (names.Length != 1)
                            {
                                firstname = names[0];
                                lastname = names[1];
                            }

                            //Message                            
                            Messages_ClassLibrary.Message_new_customer message = new Messages_ClassLibrary.Message_new_customer()
                            {
                                firstname = firstname,
                                lastname = lastname,
                                email = GetValueFromNameValueList("email", c.name_value_list),
                                birthday = "",
                                address1 = GetValueFromNameValueList("billing_address_street", c.name_value_list),
                                address2 = GetValueFromNameValueList("billing_address_street2", c.name_value_list),
                                postcode = GetValueFromNameValueList("billing_address_postalcode", c.name_value_list),
                                city = GetValueFromNameValueList("billing_address_city", c.name_value_list),
                                phone = GetValueFromNameValueList("phone_fax", c.name_value_list),
                                phone_mobile = GetValueFromNameValueList("phone_office", c.name_value_list),
                            };
                            messages[index] = message;
                            index++;

                        }

                    }

                    //Send new data
                    if (messages.Count() != 0)
                    {
                        //RabbitMq
                        ConnectionFactory factory = new ConnectionFactory();
                        factory.UserName = "guest";
                        factory.Password = "guest";
                        factory.HostName = "10.3.51.37";

                        IConnection conn = factory.CreateConnection();
                        IModel channel = conn.CreateModel();

                        channel.ExchangeDeclare(
                            exchange: "new_crm_customer_exchange",
                            type: "fanout");

                        channel.QueueDeclare(
                            queue: "crm_pos_customer_queue",
                            durable: true,
                            exclusive: false,
                            autoDelete: false,
                            arguments: null);
                        channel.QueueDeclare(
                            queue: "crm_ninja_customer_queue",
                            durable: true,
                            exclusive: false,
                            autoDelete: false,
                            arguments: null);

                        channel.QueueBind(
                            queue: "crm_pos_customer_queue",
                            exchange: "new_crm_customer_exchange",
                            routingKey: "new_crm_customer_exchange");
                        channel.QueueBind(
                            queue: "crm_ninja_customer_queue",
                            exchange: "new_crm_customer_exchange",
                            routingKey: "new_crm_customer_exchange");

                        string messageBody = JsonConvert.SerializeObject(messages);
                        byte[] messageBodyBytes = Encoding.UTF8.GetBytes(messageBody);

                        //Send
                        channel.BasicPublish(
                            exchange: "new_crm_customer_exchange",
                            routingKey: "new_crm_customer_exchange",
                            basicProperties: null,
                            body: messageBodyBytes);

                        Console.WriteLine("new data sended. @Thread: " + Thread.CurrentThread.ManagedThreadId);
                        channel.Dispose();
                        conn.Dispose();
                    }

                    actual_list = suiteCrm_all_accounts;

                }


                ///
            });
        }




        //NEW HMS CUSTOMER RECEIVER
        public static Action New_hms_customer_notified()
        {
            return new Action(() =>
            {
                //WAIT FOR NEW MESSAGES
                var factory = new ConnectionFactory();
                factory.UserName = "guest";
                factory.Password = "guest";
                factory.HostName = "10.3.51.37";

                var conn = factory.CreateConnection();
                var channel = conn.CreateModel();

                channel.ExchangeDeclare(
                    exchange: "new_hms_customer_exchange",
                    type: ExchangeType.Direct);
                channel.QueueDeclare(
                    queue: "hms_crm_customer_queue",
                    durable: true,
                    exclusive: false,
                    autoDelete: false,
                    arguments: null);
                channel.QueueBind(
                    queue: "hms_crm_customer_queue",
                    exchange: "new_hms_customer_exchange",
                    routingKey: "new_hms_customer_exchange");

                var suitecrm_consumer = new EventingBasicConsumer(channel);
                suitecrm_consumer.Received += async (IModel, ea) =>
                {
                    //PULL DATA FROM MESSAGE
                    var bodyString = Encoding.UTF8.GetString(ea.Body);
                    List<Message_new_customer> message_list = JsonConvert.DeserializeObject<List<Message_new_customer>>(bodyString.ToString());
                    await System.Threading.Tasks.Task.Run(() => New_hms_customer_received(message_list));

                    //ACK
                    channel.BasicAck(ea.DeliveryTag, false);
                };

                channel.BasicConsume(queue: "hms_crm_customer_queue", noAck: false, consumer: suitecrm_consumer);


            });
        }
        private static void New_hms_customer_received(
            List<Message_new_customer> message_list)
        {
            Console.WriteLine("\n \n!NEW message received" + "\n");

            
            string sugarCrmUsername = "User";
            string sugarCrmPassword = "6a4dc9133d5f3b6d9fff778aff361961";



            //GET SUITECRM USERS
            ServiceReference1.sugarsoapPortTypeClient soap = new ServiceReference1.sugarsoapPortTypeClient("sugarsoapPort");
            ServiceReference1.user_auth user = new ServiceReference1.user_auth();

            user.user_name = sugarCrmUsername;
            user.password = sugarCrmPassword;

            ServiceReference1.name_value[] loginList = new ServiceReference1.name_value[0];
            ServiceReference1.entry_value result_login = soap.login(user, "SOAP_RABBITMQ", loginList);

            string sessionId = result_login.id;

            ServiceReference1.get_entry_list_result_version2 suiteCrm_all_accounts = soap.get_entry_list(
                sessionId,
                "Accounts",
                "",
                "",
                0,
                new[] { "id", "name" },
                null,
                99999,
                0,
                false);



            //PROCES MESSAGE
            foreach (Message_new_customer m in message_list)
            {
                int index = 0;
                bool contain = false;
                foreach (entry_value var in suiteCrm_all_accounts.entry_list)
                {

                    string soapC_name = GetValueFromNameValueList("name", var.name_value_list);
                    string soapC_id = GetValueFromNameValueList("id", var.name_value_list);

                    //Console.WriteLine(name);


                    //UPDATE
                    if (soapC_name.Contains(m.firstname) && soapC_name.Contains(m.lastname))
                    {
                        contain = true;


                        soapC_name = m.firstname + " " + m.lastname;
                        string soapC_billingA1 = m.address1;
                        string soapC_billingA2 = m.address2;
                        string soapC_billingCity = m.city;
                        string soapC_billingState = m.city;
                        string soapC_billingPostalcode = m.postcode;
                        string soapC_billingCountry = m.city;
                        string soapC_billingPhoneM = m.phone_mobile;
                        string soapC_billingPhone = m.phone;
                        string soapC_billingEmail = m.email;

                        NameValueCollection fieldListCollection = new NameValueCollection();
                        //to update a record, you will nee to pass in a record id as commented below
                        fieldListCollection.Add("id", soapC_id);
                        fieldListCollection.Add("name", soapC_name);
                        fieldListCollection.Add("billing_address_street", soapC_billingA1);
                        fieldListCollection.Add("billing_address_street_2", soapC_billingA2);
                        fieldListCollection.Add("billing_address_city", soapC_billingCity);
                        fieldListCollection.Add("billing_address_state", soapC_billingState);
                        fieldListCollection.Add("billing_address_postalcode", soapC_billingPostalcode);
                        fieldListCollection.Add("billing_address_country", soapC_billingCountry);
                        fieldListCollection.Add("phone_office", soapC_billingPhoneM);
                        fieldListCollection.Add("phone_fax", soapC_billingPhone);
                        fieldListCollection.Add("email1", soapC_billingEmail);

                        //this is just a trick to avoid having to manually specify index values for name_value[]
                        ServiceReference1.name_value[] fieldList = new ServiceReference1.name_value[fieldListCollection.Count];

                        int count = 0;
                        foreach (string name in fieldListCollection)
                        {
                            foreach (string value in fieldListCollection.GetValues(name))
                            {
                                ServiceReference1.name_value field = new ServiceReference1.name_value();
                                field.name = name; field.value = value;
                                fieldList[count] = field;
                            }
                            count++;
                        }

                        try
                        {
                            ServiceReference1.new_set_entry_result result_insert = soap.set_entry(sessionId, "Accounts", fieldList);
                            string RecordID = result_insert.id;

                            //show record id to user
                            Console.WriteLine("=> customer updated, id: " + RecordID + "\n");
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine(ex.Message);
                            Console.WriteLine(ex.Source);
                        }
                        
                    }
                }
                
                //CREATE
                if(contain == false)
                {
                    Console.WriteLine("New customer");


                    string soapC_name = m.firstname + m.lastname;
                    string soapC_billingA1 = m.address1;
                    string soapC_billingA2 = m.address2;
                    string soapC_billingCity = m.city;
                    string soapC_billingState = m.city;
                    string soapC_billingPostalcode = m.postcode;
                    string soapC_billingCountry = m.city;
                    string soapC_billingPhoneM = m.phone_mobile;
                    string soapC_billingPhone = m.phone;
                    string soapC_billingEmail = m.email;


                    NameValueCollection fieldListCollection = new NameValueCollection();

                    fieldListCollection.Add("name", soapC_name);
                    fieldListCollection.Add("billing_address_street", soapC_billingA1);
                    fieldListCollection.Add("billing_address_street_2", soapC_billingA2);
                    fieldListCollection.Add("billing_address_city", soapC_billingCity);
                    fieldListCollection.Add("billing_address_state", soapC_billingState);
                    fieldListCollection.Add("billing_address_postalcode", soapC_billingPostalcode);
                    fieldListCollection.Add("billing_address_country", soapC_billingCountry);
                    fieldListCollection.Add("phone_office", soapC_billingPhoneM);
                    fieldListCollection.Add("phone_fax", soapC_billingPhone);
                    fieldListCollection.Add("email1", soapC_billingEmail);

                    ServiceReference1.name_value[] fieldList = new ServiceReference1.name_value[fieldListCollection.Count];
                    int count = 0;
                    foreach (string name in fieldListCollection)
                    {
                        foreach (string value in fieldListCollection.GetValues(name))
                        {
                            ServiceReference1.name_value field = new ServiceReference1.name_value();
                            field.name = name; field.value = value;
                            fieldList[count] = field;
                        }
                        count++;
                    }

                    try
                    {
                        ServiceReference1.new_set_entry_result result_insert = soap.set_entry(sessionId, "Accounts", fieldList);
                        string RecordID = result_insert.id;

                        //show record id to user
                        Console.WriteLine("=> customer added, id: " + RecordID + "\n");
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(ex.Message);
                        Console.WriteLine(ex.Source);
                    }
                }

            }



        }

        
        //NEW POS CUSTOMER RECEIVER
        public static Action New_pos_customer_notified(
          string BASE_URL, string ACCOUNT, string PASSWORD)
        {
            return new Action(() =>
            {
                //WAIT FOR NEW MESSAGES
                var factory = new ConnectionFactory();
                factory.UserName = "guest";
                factory.Password = "guest";
                factory.HostName = "10.3.51.37";

                var conn = factory.CreateConnection();
                var channel = conn.CreateModel();

                channel.ExchangeDeclare(
                    exchange: "new_crm_customer_exchange",
                    type: "fanout");
                channel.QueueDeclare(
                    queue: "crm_pos_customer_queue",
                    durable: true,
                    exclusive: false,
                    autoDelete: false,
                    arguments: null);
                channel.QueueBind(
                    queue: "crm_pos_customer_queue",
                    exchange: "new_crm_customer_exchange",
                    routingKey: "new_crm_customer_exchange");

                var suitecrm_consumer = new EventingBasicConsumer(channel);
                suitecrm_consumer.Received += async (IModel, ea) =>
                {
                    //PULL DATA FROM MESSAGE
                    var bodyString = Encoding.UTF8.GetString(ea.Body);
                    List<Message_new_customer> message_list = JsonConvert.DeserializeObject<List<Message_new_customer>>(bodyString.ToString());
                    await System.Threading.Tasks.Task.Run(() => New_pos_customer_received(message_list, BASE_URL, ACCOUNT, PASSWORD));
                    
                    //ACK
                    channel.BasicAck(ea.DeliveryTag, false);
                };

                channel.BasicConsume(queue: "crm_pos_customer_queue", noAck: false, consumer: suitecrm_consumer);


            });
        }
        private static void New_pos_customer_received(
           List<Message_new_customer> message_list,
            string BASE_URL, string ACCOUNT, string PASSWORD)
        {
            Console.WriteLine("\n \n!NEW message received. From: CRM To: POS \n");
            
            //GET POS USERS
            List<Bukimedia.PrestaSharp.Entities.customer> pos_all_customers = new List<Bukimedia.PrestaSharp.Entities.customer>();
            CustomerFactory customers1 = new CustomerFactory(BASE_URL, ACCOUNT, PASSWORD);
            pos_all_customers = customers1.GetAll();


            //PROCES MESSAGE
            foreach (Message_new_customer m in message_list)
            {
                bool contain = false;
                foreach (Bukimedia.PrestaSharp.Entities.customer customer in pos_all_customers)
                {

                    string posC_firstname = customer.firstname;
                    string posC_lastname = customer.lastname;
                    string posC_name = posC_firstname + " " + posC_lastname;
                    
                    //UPDATE
                    if ( posC_firstname.Contains(m.firstname) || posC_lastname.Contains(m.lastname)  || ( posC_name.Contains(m.firstname + m.lastname) ))
                    {
                        contain = true;

                        //CustomerFactory customers = new CustomerFactory(BASE_URL, ACCOUNT, PASSWORD);
                        //AddressFactory addresses = new AddressFactory(BASE_URL, ACCOUNT, PASSWORD);
                        //Bukimedia.PrestaSharp.Entities.address posC_address = getAdres((long)customer.id, BASE_URL, ACCOUNT, PASSWORD);
                        //customers.Update(customer);                        
                    }
                }

                //CREATE
                if (contain == false)
                {
                    CustomerFactory customers = new CustomerFactory(BASE_URL, ACCOUNT, PASSWORD);
                    Bukimedia.PrestaSharp.Entities.customer customer = new Bukimedia.PrestaSharp.Entities.customer();
                    customer.firstname = (m.firstname.Length == 0) ? m.firstname + m.lastname : m.firstname;
                    customer.lastname = (m.lastname.Length == 0) ? m.firstname + m.lastname : m.firstname;
                    customer.email = "empty@empty.com";
                    customer.passwd = "empty";
                    long RecordID = (long)customers.Add(customer).id;

                    Console.WriteLine("=> customer added, id: " + RecordID + "\n");
                }

            }



        }


        //...
        private static string GetValueFromNameValueList(
            string key, IEnumerable<name_value> nameValues)
        {
            if (nameValues.Where(nv => nv.name == key).ToArray().Count() != 0)
                return nameValues.Where(nv => nv.name == key).ToArray()[0].value;
            return "";
        }  
        
        public static Bukimedia.PrestaSharp.Entities.address getAdres(
            long customer_id, string BASE_URL, string ACCOUNT, string PASSWORD)
        {
            AddressFactory addresses = new AddressFactory(BASE_URL, ACCOUNT, PASSWORD);
            List<Bukimedia.PrestaSharp.Entities.address> all = addresses.GetAll();

            foreach (Bukimedia.PrestaSharp.Entities.address a in all)
            {
                if (a.id_customer == customer_id)
                {
                    return a;
                }

            }
            return null;
        }


    }
}
