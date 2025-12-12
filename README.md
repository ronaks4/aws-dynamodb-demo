# Amazon DynamoDB Movies Demo

This demo uses Amazon DynamoDB with Next.js to fetch movies from the database. It is able to securely connect to DynamoDB without using hardcoded access tokens through Vercel's [OIDC Federation](https://vercel.com/docs/security/secure-backend-access/oidc).

[![This is an alt text.](/app/Vercel-AWS-GitHub-DDB.png)](https://vercel.com/new/clone?repository-url=https%3A%2F%2Fgithub.com%2Fvercel%2Faws-dynamodb-demo&project-name=aws-dynamodb-demo&repository-name=aws-dynamodb-demo&products=%5B%7B%22type%22%3A%22integration%22%2C%22integrationSlug%22%3A%22aws%22%2C%22productSlug%22%3A%22aws-dynamodb%22%2C%22protocol%22%3A%22storage%22%7D%5D)

**Demo:**
QQQQQ

**Getting Started:**
Click the "Deploy" button to clone this repo, create a new Vercel project, setup the AWS integration, and provision a new DynamoDB table:

[![Deploy with Vercel](https://vercel.com/button)](https://vercel.com/new/clone?repository-url=https%3A%2F%2Fgithub.com%2Fvercel%2Faws-dynamodb-demo&project-name=aws-dynamodb-demo&repository-name=aws-dynamodb-demo&products=%5B%7B%22type%22%3A%22integration%22%2C%22integrationSlug%22%3A%22aws%22%2C%22productSlug%22%3A%22aws-dynamodb%22%2C%22protocol%22%3A%22storage%22%7D%5D)

Once the process is complete, you can clone the newly created GitHub repository and start making changes locally.

## Setup

1. Add required environment variables to your vercel project

```
AWS_ROLE_ARN=arn:aws:iam::YOUR_ACCOUNT:role
AWS_REGION=us-east-1
DYNAMODB_TABLE_NAME=your-dynamodb-table-name
```

2. Pull vercel environment variables locally

`bash
vercel env pull
`

3. Install dependencies:

```bash
npm install
```

4. Run migrations to create tables:

```bash
npm run db:migrate
```

5. Seed the database with movie data:

```bash
npm run db:seed
```

6. Start the development server:

```bash
npm run dev
```
