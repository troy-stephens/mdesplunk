using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Splunk.Helpers
{
    internal static class Utilities
    {
        public static string GetEnvironmentVariable(string name, string defaultValue = "")
        {
            string value = System.Environment.GetEnvironmentVariable(name, EnvironmentVariableTarget.Process);
            return string.IsNullOrEmpty(value) ? defaultValue : value;
        }
    }
}
