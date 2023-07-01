using Nest;
using MimeKit;
using SmtpClient = MailKit.Net.Smtp.SmtpClient;
using MongoDB.Driver;
using System.Collections.Concurrent;

namespace LogNotificationScheduler
{
    public class NotificationScheduler
    {
        private readonly IElasticClient _elasticClient;
        private readonly IMongoDatabase _database;

        public NotificationScheduler()
        {
            // Initialize your ElasticClient with appropriate connection settings
            _elasticClient = CreateElasticClient();
            _database = CreateMongoDbConnection();
        }

        public void RunScheduler()
        {
            // Get a list of all indexes in Elasticsearch
            ConcurrentDictionary<string, string> dict = new();
            var indexNames = _elasticClient.Indices.Get(Indices.All).Indices.Keys.Where(x => x.Name.StartsWith("customLogger")).ToList();
            //Get Mappings of emails from mongo for each index.
            var collection = _database.GetCollection<Registration>("registrations");
            foreach (var index in indexNames)
            {
                var data = collection.Find(x => x.RegistrationId == index.Name);
                var filter = Builders<Registration>.Filter.Eq(r => r.RegistrationId, index.Name);
                var registration = collection.Find(filter).FirstOrDefault();
                if (!dict.ContainsKey(index.Name))
                {
                    dict.TryAdd(index.Name, data.First().EmailId);
                }
                else
                {
                    dict[index.Name] = data.First().EmailId;
                }
            }
            // Query each index for log messages and filter based on conditions
            try
            {
                foreach (IndexName indexName in indexNames.Distinct())
                {
                    ISearchResponse<LogMessage> searchResponse;
                    var filter = Builders<Registration>.Filter.Eq(r => r.RegistrationId, indexName.Name);
                    var registration = collection.Find(filter).FirstOrDefault();
                    Registration validRegistration = registration ?? throw new Exception("No registration found for the specified condition.");
                    if (validRegistration != null)
                    {
                        var lastAlertTime = validRegistration.LastEmailAlert;
                        int notificationThreshold = validRegistration.ErrorAlerts;
                        if (lastAlertTime == default)
                        {
                            searchResponse = _elasticClient.Search<LogMessage>(s => s
                                .Index(indexName)
                                .Query(q => q.MultiMatch(x => x.Fields(z => z.Field(e => e.Level).Field(e => e.Message)).Query("Error")))
                            );
                        }
                        else
                        {
                            searchResponse = _elasticClient.Search<LogMessage>(s => s
                                              .Index(indexName)
                                              .Query(q => q
                                                  .DateRange(dr => dr
                                                      .Field(f => f.TimeStamp)
                                                      .GreaterThanOrEquals(lastAlertTime)
                                                  )
                                              ).Query(q => q.MultiMatch(x => x.Fields(z => z.Field(e => e.Level).Field(e => e.Message)).Query("Error")))
                                              );
                        }

                        if ((int)searchResponse.Total >= notificationThreshold)
                        {
                            // Send notification via email
                            string recipientEmail = dict[indexName.Name];
                            string subject = $"Error Notification for your Registration Id: {indexName.Name}";
                            string body = $"Number of errors: {(int)searchResponse.Total}. Please check the logs and visualize from the website.";

                            SendEmail(recipientEmail, subject, body);
                            validRegistration.LastEmailAlert = DateTime.Now;
                            validRegistration.Emails.Add(new Email { To = recipientEmail, From = "aptandon11995@gmail.com", Subject = subject, Body = body });
                            var update = Builders<Registration>.Update.Set(r => r.LastEmailAlert, validRegistration.LastEmailAlert)
                                                     .Set(r => r.Emails, validRegistration.Emails);
                            collection.UpdateOne(filter, update);
                        }
                    }
                }
            }
            catch (Exception e)
            {

            }
        }

        private static void SendEmail(string recipientEmail, string subject, string body)
        {
            var message = new MimeMessage();
            message.From.Add(new MailboxAddress("Logger Admin", "aptandon11995@gmail.com"));
            message.To.Add(new MailboxAddress("Recipient Name", recipientEmail));
            message.Subject = subject;
            message.Body = new TextPart("plain")
            {
                Text = body
            };

            using var client = new SmtpClient();
            client.Connect("smtp.gmail.com", 465, true);
            client.AuthenticationMechanisms.Remove("XOAUTH2");
            client.Authenticate("aptandon11995@gmail.com", "affekyhfqwmfgmyr");
            client.Send(message);
            client.Disconnect(true);
        }

        private static IElasticClient CreateElasticClient()
        {
            var connectionString = "http://localhost:9200/";
            var settings = new ConnectionSettings(new Uri(connectionString));
            var elasticClient = new ElasticClient(settings);
            return elasticClient;
        }

        private static IMongoDatabase CreateMongoDbConnection()
        {
            var connectionString = "mongodb+srv://loggerNuget:loggerNuget@loggernugetregistration.3wyibtf.mongodb.net/?retryWrites=true&w=majority";
            var settings = MongoClientSettings.FromConnectionString(connectionString);
            settings.ServerApi = new ServerApi(ServerApiVersion.V1);
            var client = new MongoClient(settings);
            return client.GetDatabase("loggerRegistration");
        }

        public class LogMessage
        {
            public string? Level { get; set; }
            public string? Message { get; set; }
            public DateTime TimeStamp { get; set; }
        }
    }
}
