import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import {
  BatchWriteCommand,
  DynamoDBDocumentClient,
} from "@aws-sdk/lib-dynamodb";
import { awsCredentialsProvider } from "@vercel/functions/oidc";
import fs from "fs";
import path from "path";
import csv from "csv-parser";
import { config } from "dotenv";

config({ path: ".env.local" });
config();

async function readMovieTitlesFromCSV(): Promise<string[]> {
  const movieTitles = new Set<string>();
  const csvFilePath = path.resolve(__dirname, "movies.csv");

  return new Promise((resolve, reject) => {
    fs.createReadStream(csvFilePath)
      .pipe(csv())
      .on("data", (row) => {
        const title = row.title?.trim();
        if (title) {
          movieTitles.add(title);
        }
      })
      .on("end", () => {
        const uniqueMovieTitles = Array.from(movieTitles);
        console.log(
          `Parsed ${uniqueMovieTitles.length} unique movies from CSV.`,
        );
        resolve(uniqueMovieTitles);
      })
      .on("error", (error) => {
        console.error("Error reading CSV file:", error);
        reject(error);
      });
  });
}

async function main() {
  const movieTitles = await readMovieTitlesFromCSV();
  const defaultDate = new Date("2024-12-07");
  const batchSize = 25;
  const partitionKey = process.env.DYNAMODB_TABLE_PARTITION_KEY || "PK";
  const sortKey = process.env.DYNAMODB_TABLE_SORT_KEY || "SK";


  const client = new DynamoDBClient({
    region: process.env.AWS_REGION!,
    credentials: awsCredentialsProvider({
      roleArn: process.env.AWS_ROLE_ARN!,
      clientConfig: { region: process.env.AWS_REGION! },
    }),
  });
  const docClient = DynamoDBDocumentClient.from(client);

  for (let i = 0; i < movieTitles.length; i += batchSize) {
    try {
      const batch = movieTitles.slice(i, i + batchSize).map((title, index) => {
        const movieId = (i + index + 1).toString();
        return {
          [partitionKey]: `MOVIE#${movieId}`,
          [sortKey]: `MOVIE#${movieId}`,
          entityType: "movie",
          id: movieId,
          title,
          score: 0,
          lastVoteTime: defaultDate.toISOString(),
        };
      });

      await batchWriteItems(docClient, batch);

      console.log(
        `Inserted ${Math.min(i + batchSize, movieTitles.length)} / ${
          movieTitles.length
        } movies`,
      );
    } catch (error) {
      console.error("Batch insert error:", error);
      throw error;
    }
  }

  console.log(`Seeded ${movieTitles.length} movies`);
  process.exit();
}

async function batchWriteItems(client: DynamoDBDocumentClient, items: any[]) {
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
    const data = await client.send(command);
    return data;
  } catch (error) {
    console.error("BatchWrite error:", error);
    throw error;
  }
}

main().catch((error) => {
  console.error("Seeding failed:", error);
  process.exit(1);
});
