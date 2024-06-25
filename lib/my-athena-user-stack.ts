import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as s3 from 'aws-cdk-lib/aws-s3';

export class MyAthenaUserStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    // Criação do bucket S3 para salvar os resultados das consultas
    const queryResultsBucket = new s3.Bucket(this, 'QueryResultsBucket', {
      removalPolicy: cdk.RemovalPolicy.DESTROY, // Remover o bucket quando o stack for destruído
    });

    // Criação do usuário IAM
    const athenaUser = new iam.User(this, 'AthenaUser', {
      userName: 'athena-prontuario',
    });

    // Políticas gerenciadas necessárias para acessar o Athena
    athenaUser.addManagedPolicy(iam.ManagedPolicy.fromAwsManagedPolicyName('AmazonAthenaFullAccess'));
    
    // Políticas adicionais que podem ser necessárias (por exemplo, acesso ao S3 onde estão os dados)
    athenaUser.addManagedPolicy(iam.ManagedPolicy.fromAwsManagedPolicyName('AmazonS3ReadOnlyAccess'));

    // Política customizada para permitir que o usuário escreva no bucket S3
    const s3WritePolicy = new iam.PolicyStatement({
      actions: [
        's3:PutObject',
        's3:GetObject',
        's3:ListBucket',
      ],
      resources: [
        queryResultsBucket.bucketArn,
        `${queryResultsBucket.bucketArn}/*`,
      ],
    });

    // Anexar a política customizada ao usuário
    athenaUser.addToPolicy(s3WritePolicy);

    // Criação da role IAM para o Superset
    const supersetRole = new iam.Role(this, 'SupersetRole', {
      assumedBy: new iam.ServicePrincipal('ec2.amazonaws.com'),
      description: 'Role for Superset to access Athena and S3',
    });

    // Anexar as políticas gerenciadas necessárias à role
    supersetRole.addManagedPolicy(iam.ManagedPolicy.fromAwsManagedPolicyName('AmazonAthenaFullAccess'));
    supersetRole.addManagedPolicy(iam.ManagedPolicy.fromAwsManagedPolicyName('AmazonS3ReadOnlyAccess'));

    // Anexar a política customizada à role
    supersetRole.addToPolicy(s3WritePolicy);

    // Output para exibir o nome do bucket S3
    new cdk.CfnOutput(this, 'QueryResultsBucketName', {
      value: queryResultsBucket.bucketName,
      description: 'Bucket to store Athena query results',
    });

    // Output para exibir o ARN da role do Superset
    new cdk.CfnOutput(this, 'SupersetRoleArn', {
      value: supersetRole.roleArn,
      description: 'ARN of the IAM role for Superset',
    });
  }
}
