using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using FileToAzureServiceBus;
using System.Collections.Generic;

namespace TestProject
{
    [TestClass]
    public class UnitTest1
    {
        [TestMethod]
        public void TestMethod1()
        {
            List<int> someList = new List<int>();
            someList.Add(1);
            someList.Add(1);
            someList.Add(1);
            someList.Add(1);
            someList.Add(1);
            someList.Add(1);
            someList.Add(1);

            var result = StaticHelper.SplitTest<int>(someList.ToArray(), 2);

            Assert.AreEqual(1, 1);
        }
    }
}
