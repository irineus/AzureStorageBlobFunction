using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System.IO;
using System.Threading.Tasks;

namespace AzureStorageBlobFunction
{
    public class ProcessStorageBlob
    {
        [FunctionName("ProcessStorageBlob")]
        public async Task RunAsync([BlobTrigger("bemol-images/{name}", Connection = "AzureStorageConnectionString")]Stream myBlob, string name, ILogger log)
        {
            log.LogInformation($"Blob trigger function starting for blob\n\t Name:{name} \n\t Size: {myBlob.Length} Bytes");
            await CopyMetadataToTagAsync("bemol-images", name, "cliente_id", log);
            log.LogInformation($"*** Blob trigger function ended for blob '{name}' ***");
        }

        public static async Task CopyMetadataToTagAsync(string container, string blobName, string metadataKey, ILogger log)
        {
            var builder = new ConfigurationBuilder()
               .SetBasePath(Directory.GetCurrentDirectory())
               .AddJsonFile("local.settings.json", optional: false);

            var config = builder.Build();

            string storageConnString = config.GetSection("Values:AzureStorageConnectionString").Value;

            BlobContainerClient containerClient = new BlobContainerClient(storageConnString, container);
            //Read Blob Metadata
            BlobClient blobClient = containerClient.GetBlobClient(blobName);
            BlobProperties properties = await blobClient.GetPropertiesAsync();
            foreach (var metadataItem in properties.Metadata)
            {
                if (metadataItem.Key == metadataKey)
                {
                    log.LogInformation($"Found Metadata Key: '{metadataItem.Key}'");
                    //Check if exists a Tag with the same Metadata Key
                    GetBlobTagResult blobTags = await blobClient.GetTagsAsync();
                    if (blobTags.Tags.ContainsKey(metadataItem.Key))
                    {
                        log.LogInformation($"Tag '{metadataItem.Key}' already exists. Recreating with the same value.");
                        blobTags.Tags.Remove(metadataItem.Key);
                    }
                    await blobClient.SetTagsAsync(properties.Metadata);
                    log.LogInformation($"Tag '{metadataItem.Key}' created with value {metadataItem.Value}");
                }
            }
        }

    }
}
