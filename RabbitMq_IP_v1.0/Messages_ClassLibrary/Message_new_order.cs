using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Messages_ClassLibrary
{
    class Message_new_order
    {
        public string Sender { get; set; }


        public class Order
        {
            public class Product
            {
                public string Name { get; set; }
                public string Description { get; set; }
                public decimal Quantity { get; set; }
                public decimal Price { get; set; }
                public decimal Price_tax_incl { get; set; }
            }

            public string Date_invoice { get; set; }
            public string Payment_method { get; set; }

            public decimal Discount { get; set; }
            public decimal Shipping { get; set; }
            public decimal Total { get; set; }


            public decimal Discount_taxt_incl { get; set; }
            public decimal Shipping_tax_incl { get; set; }
            public decimal Total_tax_incl { get; set; }


            //PRODUCTS
            public Product[] Producten { get; set; }

        }
        //CUSTOMER
        public string Firstname { get; set; }
        public string Lastname { get; set; }
        public string Email { get; set; }
        public string Birthday { get; set; }
        public string Address1 { get; set; }
        public string Address2 { get; set; }
        public string Postcode { get; set; }
        public string City { get; set; }
        public string Phone { get; set; }
        public string Phone_mobile { get; set; }


        //ORDERS
        public Order[] Orders { get; set; }
    }

}
