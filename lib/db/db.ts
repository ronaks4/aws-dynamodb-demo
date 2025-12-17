import { awsCredentialsProvider } from "@vercel/functions/oidc";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import {
  DynamoDBDocumentClient,
  BatchWriteCommand,
  ScanCommand,
} from "@aws-sdk/lib-dynamodb";

const client = new DynamoDBClient({
  region: process.env.AWS_REGION || "us-east-1",
  credentials: awsCredentialsProvider({
    roleArn: process.env.AWS_ROLE_ARN!,
  }),
});

const docClient = DynamoDBDocumentClient.from(client);

export function getClient() {
  return docClient;
}

export async function batchWriteItems(items: any[]) {
  const tableName = process.env.DYNAMODB_TABLE_NAME;

  if (!tableName) {
    throw new Error("DYNAMODB_TABLE_NAME environment variable is required");
  }

  const writeRequests = items.map((item) => ({
    PutRequest: { Item: item },
  }));

  const params = {
    RequestItems: {
      [tableName]: writeRequests,
    },
  };

  try {
    const command = new BatchWriteCommand(params);
    const data = await docClient.send(command);
    return data;
  } catch (error) {
    console.error("BatchWrite error:", error);
    throw error;
  }
}

export async function scanTable() {
  const tableName = process.env.DYNAMODB_TABLE_NAME;

  if (!tableName) {
    throw new Error("DYNAMODB_TABLE_NAME environment variable is required");
  }

  const params = {
    TableName: tableName,
  };

  try {
    const command = new ScanCommand(params);
    const data = await docClient.send(command);
    return data;
  } catch (error) {
    console.error("Scan error:", error);
    throw error;
  }
}
