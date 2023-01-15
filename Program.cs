using System.Diagnostics;
using Bogus;
using Nest;

var clusterUrl = ""; //Elasticsearch server url;
var elasticServerCertificateFingerprint = ""; //Elasticsearch server certificate fingerprint
var elasticServerUser = ""; //Elasticsearch server username
var elasticServerPassword = ""; //Elasticsearch server user password
var indexName = ""; //Index to use in the Elasticsearch server

var defaultConsoleColor = Console.ForegroundColor;
Console.ForegroundColor = ConsoleColor.Green;


Console.WriteLine("Connecting to elasticseach server ...");

var connectionSettings = new ConnectionSettings(new System.Uri(clusterUrl));
connectionSettings.CertificateFingerprint(elasticServerCertificateFingerprint);
connectionSettings.BasicAuthentication(elasticServerUser, elasticServerPassword);
connectionSettings.EnableApiVersioningHeader();
// This is going to enable us to see the raw queries sent to elastic when debugging (really useful)
connectionSettings.EnableDebugMode();

// Create the actual client
var client = new ElasticClient(connectionSettings);

Console.WriteLine("Connection established!");

var BuildFullTextSearchIndex = (ElasticClient client) =>
{
    var index = client.Indices.Create(indexName, i => i
                    .Settings(s => s
                        .Analysis(a => a
                            .TokenFilters(tf => tf
                                .Stop("spanish_stop", stop => stop
                                    .StopWords("_spanish_"))
                                .Stemmer("spanish_stemmer", stemmer => stemmer
                                    .Language("spanish"))
                                .Phonetic("phonetic", descriptor => descriptor
                                    .LanguageSet(PhoneticLanguage.Spanish)
                                    .RuleType(PhoneticRuleType.Approximate)
                                    .Encoder(PhoneticEncoder.Metaphone)
                                    .Replace(true))
                            )
                            .Analyzers(aa => aa
                                .Custom("custom_phonetic", ca => ca
                                    .CharFilters("html_strip")
                                    .Tokenizer("standard")
                                    .Filters("lowercase", "asciifolding", "spanish_stop", "spanish_stemmer", "phonetic")
                                )
                                .Custom("custom_spanish", ca => ca
                                    .CharFilters("html_strip")
                                    .Tokenizer("standard")
                                    .Filters("lowercase")
                                )
                            )
                        )
                    )
                    .Map<IndexedDocument>(map => map
                       .AutoMap()
                       .Properties(ps => ps
                           .Nested<DocumentKeyValuePair>(kv => kv
                               .Name(p => p.DocumentData)
                               .AutoMap()
                               .Properties(jo => jo
                                   .Keyword(t => t
                                       .Name(k => k.Key))
                                   .Date(d => d
                                       .Name(n => n.DateValue))
                                   .Number(n => n
                                       .Name(n => n.NumericValue))
                                   .Text(t => t
                                       .Name(n => n.TextValue)
                                       .Analyzer("custom_spanish"))
                                    .Text(t => t
                                        .Name(n => n.PhoneticTextValue)
                                        .Analyzer("custom_phonetic"))
                                )
                            )
                        )
                    )
                );
};


Console.WriteLine("Checking index existence ...");
if (client.Indices.Exists(indexName).Exists)
{
    client.Indices.Delete(indexName);
    Console.WriteLine("Index removed!");
}

Console.WriteLine("Building search index ...");
BuildFullTextSearchIndex(client);
Console.WriteLine("Index built!");



var indexedDocuments = 0;
var maxIndexedDocuments = 500000;
long mostExpensiveIndex = 0;
var stopWatch = new Stopwatch();
long elapsedMilliseconds = 0;

var faker = new Faker("es");

Console.WriteLine($"Starting load test indexing {maxIndexedDocuments} documents in an empty index");
Console.WriteLine("Indexing documents ...");
Console.ForegroundColor = ConsoleColor.Gray;

var indexedDocumentIds = new List<Guid>(maxIndexedDocuments);
var indexedDocumentNames = new List<string>(maxIndexedDocuments);
var indexedDocumentAge = new List<int>(maxIndexedDocuments);

while (indexedDocuments < maxIndexedDocuments)
{
    var fakeName = faker.Name.FirstName();
    var fakeAge = faker.Random.Int(1, 120);
    var documentId = Guid.NewGuid();
    var newDocument = new IndexedDocument()
    {
        Id = documentId,
        DocumentData = new List<DocumentKeyValuePair>() {
            new DocumentKeyValuePair() {
                Key = "name",
                TextValue =fakeName,
                PhoneticTextValue = fakeName

            },new DocumentKeyValuePair() {
                Key = "age",
                NumericValue = fakeAge

            }
        }
    };
    stopWatch.Reset();
    stopWatch.Start();
    var indexResponse = await client.IndexAsync(new IndexRequest<IndexedDocument>(newDocument, indexName, newDocument.Id));
    stopWatch.Stop();
    indexedDocumentIds.Add(documentId);
    indexedDocumentNames.Add(fakeName);
    indexedDocumentAge.Add(fakeAge);
    elapsedMilliseconds += stopWatch.ElapsedMilliseconds;
    if (stopWatch.ElapsedMilliseconds > mostExpensiveIndex)
    {
        mostExpensiveIndex = stopWatch.ElapsedMilliseconds;
    }
    indexedDocuments++;
    if (indexedDocuments % 10000 == 0)
    {
        Console.WriteLine($"{indexedDocuments} documents indexed. Total time: {(elapsedMilliseconds / 1000d) / 60d} minutes");
    }
}

Console.ForegroundColor = ConsoleColor.Yellow;
Console.WriteLine($"Load test ended with {indexedDocuments} documents indexed");
Console.WriteLine($"Total time: {(elapsedMilliseconds / 1000d) / 60d} minutes");
Console.WriteLine($"Average indexing time: {elapsedMilliseconds / (decimal)indexedDocuments} milliseconds");
Console.WriteLine($"Most expensive document indexing time: {(mostExpensiveIndex / 1000d)} seconds");

Console.ForegroundColor = ConsoleColor.Green;
Console.WriteLine($"Starting exact search by Id test");
long totalExactSearchMilliseconds = 0;
long mostExpensiveExactSearchById = 0;
var searchAttempts = 30;
var indexSearched = new List<int>();
for (int i = 0; i < searchAttempts; i++)
{
    Console.ForegroundColor = ConsoleColor.Gray;
    Console.WriteLine($"Search attempt{i + 1}");

    var randomDocumentIndex = faker.Random.Int(0, indexedDocumentIds.Count - 1);

    while (indexSearched.Contains(randomDocumentIndex))
    {
        randomDocumentIndex = faker.Random.Int(0, indexedDocumentIds.Count - 1);
    }
    indexSearched.Add(randomDocumentIndex);

    Console.WriteLine($"Searching for {indexedDocumentIds[randomDocumentIndex]}");
    stopWatch.Reset();
    stopWatch.Start();

    var result = await client.SearchAsync<IndexedDocument>(s => s
                        .Index(indexName)
                        .Query(q => q
                            .Term(t => t
                                .Field(f => f.Id)
                                .Value(indexedDocumentIds[randomDocumentIndex])
                            )
                        )
                    );
    stopWatch.Stop();
    totalExactSearchMilliseconds += stopWatch.ElapsedMilliseconds;

    if (stopWatch.ElapsedMilliseconds > mostExpensiveExactSearchById)
    {
        mostExpensiveExactSearchById = stopWatch.ElapsedMilliseconds;
    }

    Console.WriteLine($"Search result is valid: {result.IsValid}");
    Console.WriteLine($"Documents found: {result.Total}");
    Console.WriteLine($"Documents: {string.Join("|", result.Documents.Select(d => $"Id: {d.Id}, Name: {d.DocumentData.First(data => data.Key == "name").TextValue}, Age: {d.DocumentData.First(data => data.Key == "age").NumericValue}"))}");
}

Console.ForegroundColor = ConsoleColor.Yellow;
Console.WriteLine($"Exact Id search test ended with {searchAttempts} search attempts");
Console.WriteLine($"Average search time: {totalExactSearchMilliseconds / (decimal)searchAttempts} milliseconds");
Console.WriteLine($"Most expensive exact search by Id time: {(mostExpensiveExactSearchById / 1000d)} seconds");


Console.ForegroundColor = ConsoleColor.Green;
Console.WriteLine($"Starting exact search by name test");
long totalExactByNameSearchMilliseconds = 0;
long mostExpensiveExactByNameSearchById = 0;
var searchByNameAttempts = 30;
indexSearched = new List<int>();
for (int i = 0; i < searchByNameAttempts; i++)
{
    Console.ForegroundColor = ConsoleColor.Gray;
    Console.WriteLine($"Search attempt {i + 1}");

    var randomDocumentIndex = faker.Random.Int(0, indexedDocumentIds.Count - 1);

    while (indexSearched.Contains(randomDocumentIndex))
    {
        randomDocumentIndex = faker.Random.Int(0, indexedDocumentIds.Count - 1);
    }
    indexSearched.Add(randomDocumentIndex);

    Console.WriteLine($"Searching for {indexedDocumentNames[randomDocumentIndex]}");
    stopWatch.Reset();
    stopWatch.Start();

    var result = await client.SearchAsync<IndexedDocument>(s => s
                        .Index(indexName)
                        .Query(q => q
                            .Nested(n => n
                            .Path(p => p
                                .DocumentData)
                            .Query(nq => nq
                                .Bool(b => b
                                    .Must(
                                        s => s
                                        .Term(m => m
                                            .Field(f => f.DocumentData.First().Key)
                                            .Value("name")
                                        ),
                                        s => s
                                        .Term(m => m
                                            .Field(f => f.DocumentData.First().TextValue)
                                            .Value(indexedDocumentNames[randomDocumentIndex].ToLower())
                                        )
                                    )
                                )
                            )
                        )
                        )
                    );
    stopWatch.Stop();
    totalExactByNameSearchMilliseconds += stopWatch.ElapsedMilliseconds;

    if (stopWatch.ElapsedMilliseconds > mostExpensiveExactByNameSearchById)
    {
        mostExpensiveExactByNameSearchById = stopWatch.ElapsedMilliseconds;
    }

    Console.WriteLine($"Search result is valid: {result.IsValid}");
    Console.WriteLine($"Documents found: {result.Total}");
    Console.WriteLine($"Documents: {string.Join("|", result.Documents.Select(d => $"Id: {d.Id}, Name: {d.DocumentData.First(data => data.Key == "name").TextValue}"))}");
}

Console.ForegroundColor = ConsoleColor.Yellow;
Console.WriteLine($"Exact name search test ended with {searchByNameAttempts} search attempts");
Console.WriteLine($"Average search time: {totalExactByNameSearchMilliseconds / (decimal)searchByNameAttempts} milliseconds");
Console.WriteLine($"Most expensive exact search by name time: {(mostExpensiveExactByNameSearchById / 1000d)} seconds");


Console.ForegroundColor = ConsoleColor.Green;
Console.WriteLine($"Starting range search by age test. (top 10 search results will be taken)");
long totalRangeSearchMilliseconds = 0;
long mostExpensiveRangeSearch = 0;
var searchRangeAttempts = 30;
indexSearched = new List<int>();
for (int i = 0; i < searchRangeAttempts; i++)
{
    Console.ForegroundColor = ConsoleColor.Gray;
    Console.WriteLine($"Search attempt {i + 1}");

    var randomDocumentIndex = faker.Random.Int(0, indexedDocumentIds.Count - 1);

    while (indexSearched.Contains(randomDocumentIndex))
    {
        randomDocumentIndex = faker.Random.Int(0, indexedDocumentIds.Count - 1);
    }
    indexSearched.Add(randomDocumentIndex);

    Console.WriteLine($"Searching for {indexedDocumentAge[randomDocumentIndex]} - {indexedDocumentAge[randomDocumentIndex] + 10}");
    stopWatch.Reset();
    stopWatch.Start();

    var result = await client.SearchAsync<IndexedDocument>(s => s
                        .Index(indexName)
                        .Take(10)
                        .Query(q => q
                            .Nested(n => n
                                .Path(p => p
                                    .DocumentData)
                                .Query(nq => nq
                                    .Bool(b => b
                                        .Must(
                                            m => m.Term(f => f.DocumentData.First().Key, "age"),
                                            m => m.Range(r => r
                                                .Field(f => f.DocumentData.First().NumericValue)
                                                .GreaterThanOrEquals(indexedDocumentAge[randomDocumentIndex])
                                                .LessThanOrEquals(indexedDocumentAge[randomDocumentIndex] + 10)
                                            )
                                        )
                                    )
                                )
                            )
                        )
                    );
    stopWatch.Stop();
    totalRangeSearchMilliseconds += stopWatch.ElapsedMilliseconds;

    if (stopWatch.ElapsedMilliseconds > mostExpensiveRangeSearch)
    {
        mostExpensiveRangeSearch = stopWatch.ElapsedMilliseconds;
    }

    Console.WriteLine($"Search result is valid: {result.IsValid}");
    Console.WriteLine($"Documents found: {result.Total}");
    Console.WriteLine($"Documents: {string.Join("|", result.Documents.Select(d => $"Id: {d.Id}, Age: {d.DocumentData.First(data => data.Key == "age").NumericValue}"))}");
}

Console.ForegroundColor = ConsoleColor.Yellow;
Console.WriteLine($"Range search test ended with {searchRangeAttempts} search attempts");
Console.WriteLine($"Average search time: {totalRangeSearchMilliseconds / (decimal)searchRangeAttempts} milliseconds");
Console.WriteLine($"Most expensive range search time: {(mostExpensiveRangeSearch / 1000d)} seconds");


Console.ForegroundColor = ConsoleColor.Green;
Console.WriteLine($"Starting phonetic search test. (top 10 search results will be taken)");
long totalPhoneticSearchMilliseconds = 0;
long mostExpensivePhoneticSearch = 0;
var searchPhoneticAttempts = 30;
indexSearched = new List<int>();
for (int i = 0; i < 30; i++)
{
    Console.ForegroundColor = ConsoleColor.Gray;
    Console.WriteLine($"Search attempt {i + 1}");

    var randomDocumentIndex = faker.Random.Int(0, indexedDocumentIds.Count - 1);

    while (indexSearched.Contains(randomDocumentIndex))
    {
        randomDocumentIndex = faker.Random.Int(0, indexedDocumentIds.Count - 1);
    }
    indexSearched.Add(randomDocumentIndex);

    Console.WriteLine($"Searching for {indexedDocumentNames[randomDocumentIndex]}");
    stopWatch.Reset();
    stopWatch.Start();

    var result = await client.SearchAsync<IndexedDocument>(s => s
                        .Index(indexName)
                        .Take(10)
                        .Query(q => q
                            .Nested(n => n
                                .Path(p => p
                                    .DocumentData)
                                .Query(nq => nq
                                    .Bool(b => b
                                    .Should(s => s
                                        .Match(m => m
                                            .Fuzziness(Nest.Fuzziness.EditDistance(2))
                                            .Field(f => f.DocumentData.First().TextValue)
                                            .Query(indexedDocumentNames[randomDocumentIndex].ToLower())
                                        ),
                                        s => s
                                        .Match(m => m
                                            .Field(f => f.DocumentData.First().PhoneticTextValue)
                                            .Query(indexedDocumentNames[randomDocumentIndex].ToLower())
                                        )
                                    )
                                )
                                )
                            )
                        )
                    );
    stopWatch.Stop();
    totalPhoneticSearchMilliseconds += stopWatch.ElapsedMilliseconds;

    if (stopWatch.ElapsedMilliseconds > mostExpensivePhoneticSearch)
    {
        mostExpensivePhoneticSearch = stopWatch.ElapsedMilliseconds;
    }

    Console.WriteLine($"Search result is valid: {result.IsValid}");
    Console.WriteLine($"Documents found: {result.Total}");
    Console.WriteLine($"Documents: {string.Join("|", result.Documents.Select(d => $"Id: {d.Id}, Name: {d.DocumentData.First(data => data.Key == "name").TextValue}"))}");
}

Console.ForegroundColor = ConsoleColor.Yellow;
Console.WriteLine($"Phonetic search test ended with {searchPhoneticAttempts} search attempts");
Console.WriteLine($"Average search time: {totalPhoneticSearchMilliseconds / (decimal)searchPhoneticAttempts} milliseconds");
Console.WriteLine($"Most expensive phonetic search time: {(mostExpensivePhoneticSearch / 1000d)} seconds");


Console.ForegroundColor = ConsoleColor.Green;
Console.WriteLine($"Showing basic capabilities of phonetic search. (top 10 search results will be taken)");

Console.ForegroundColor = ConsoleColor.Gray;

var testNames = new string[2] { "ugo", "joze" };

foreach (var testName in testNames)
{
    stopWatch.Reset();
    stopWatch.Start();
    var phoneticTest = await client.SearchAsync<IndexedDocument>(s => s
                        .Index(indexName)
                        .Take(10)
                        .Query(q => q
                            .Nested(n => n
                                .Path(p => p
                                    .DocumentData)
                                .Query(nq => nq
                                    .Bool(b => b
                                    .Should(s => s
                                        .Match(m => m
                                            .Fuzziness(Nest.Fuzziness.EditDistance(2))
                                            .Field(f => f.DocumentData.First().TextValue)
                                            .Query(testName)
                                        ),
                                        s => s
                                        .Match(m => m
                                            .Field(f => f.DocumentData.First().PhoneticTextValue)
                                            .Query(testName)
                                        )
                                    )
                                )
                                )
                            )
                        )
                    );
    stopWatch.Stop();
    Console.WriteLine($"Search with: {testName}");
    Console.WriteLine($"Search time: {(stopWatch.ElapsedMilliseconds / 1000d)} seconds");
    Console.WriteLine($"Search result is valid: {phoneticTest.IsValid}");
    Console.WriteLine($"Documents found: {phoneticTest.Total}");
    Console.WriteLine($"Documents: {string.Join("|", phoneticTest.Documents.Select(d => $"Id: {d.Id}, Name: {d.DocumentData.First(data => data.Key == "name").TextValue}"))}");
}


Console.ForegroundColor = ConsoleColor.Green;
Console.WriteLine($"---- All tests completed ----");
client.Indices.Delete(indexName);
Console.WriteLine("Index removed!");
Console.ForegroundColor = defaultConsoleColor;