const { DefaultAzureCredential } = require("@azure/identity");
const { BlobServiceClient } = require("@azure/storage-blob");
const csvParser = require('csv-parser');
const stream = require('stream');

// Define constants for better reusability
const STORAGE_ACCOUNT_NAME = 'kirtanteststorageaccount';
const CONTAINER_NAME = 'kirtantestcontainer';
const BLOB_NAME = 'kirtantest.csv';

async function downloadBlob(blobClient) {
    const response = await blobClient.download();
    if (!response.readableStreamBody) {
        throw new Error("Blob not found");
    }
    return stream.Readable.from(response.readableStreamBody);
}

module.exports = async function (context, req) {
    try {
        const credential = new DefaultAzureCredential();
        const blobServiceClient = new BlobServiceClient(
            `https://${STORAGE_ACCOUNT_NAME}.blob.core.windows.net`,
            credential
        );

        const containerClient = blobServiceClient.getContainerClient(CONTAINER_NAME);
        const blobClient = containerClient.getBlobClient(BLOB_NAME);

        const readStream = await downloadBlob(blobClient);
        let data = [];

        await new Promise((resolve, reject) => {
            readStream.pipe(csvParser())
                .on('data', (row) => {
                    data.push(row);
                })
                .on('end', resolve)
                .on('error', reject);
        });

        context.res = {
            body: {
                status: 200,
                data: JSON.stringify(data)
            }
        };

    } catch (err) {
        context.res = {
            status: 500,
            body: `An error occurred: ${err.message}`
        };
    }
};
