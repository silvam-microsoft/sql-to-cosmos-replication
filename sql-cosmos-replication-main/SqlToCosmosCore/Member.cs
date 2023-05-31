using System;
using System.Collections.Generic;
using System.Text;

using Newtonsoft.Json;

namespace SQLtoCosmosDB
{
    //public class Member
    //{
    //    public string GroupId;

    //    [JsonProperty("id")]
    //    public string MemberId;
    //    public string Name;
    //    public string Address;
    //    public string Email;
    //    public string City;
    //    public string StateProvince;
    //    public string CountryCode;
    //    public string PostalCode;

    //    public override string ToString()
    //    {
    //        return JsonConvert.SerializeObject(this);
    //    }
    //}
    public class Member
    {
        [JsonProperty("id")]
        public string MemberId;
        public string GroupId;
        public string Name;
        public string Address;
        public EmailAddress[] EmailAddresses;
        public string Description;

        public override string ToString()
        {
            return JsonConvert.SerializeObject(this);
        }
    }

    public class EmailAddress
    {
        public string Type;
        public string Email;
    }
}

