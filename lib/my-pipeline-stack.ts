import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as codepipeline from 'aws-cdk-lib/aws-codepipeline';
import * as codepipeline_actions from 'aws-cdk-lib/aws-codepipeline-actions';
import * as codebuild from 'aws-cdk-lib/aws-codebuild';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as iam from 'aws-cdk-lib/aws-iam';

export class MyPipelineStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    // Criação do bucket S3 para artefatos do pipeline
    const artifactBucket = new s3.Bucket(this, 'PipelineArtifactBucket', {
      removalPolicy: cdk.RemovalPolicy.DESTROY, // Remover o bucket quando o stack for destruído
    });

    // Bucket S3 para armazenar scripts de ETL do Glue
    const glueScriptsBucket = new s3.Bucket(this, 'GlueScriptsBucket', {
      removalPolicy: cdk.RemovalPolicy.RETAIN, // Manter o bucket quando o stack for destruído
    });

    // Definir as variáveis de entrada do pipeline
    const sourceOutput = new codepipeline.Artifact();
    const cdkBuildOutput = new codepipeline.Artifact();
    const glueScriptOutput = new codepipeline.Artifact();

    // Role do IAM para o CodePipeline acessar o Secrets Manager
    const pipelineRole = new iam.Role(this, 'PipelineRole', {
      assumedBy: new iam.ServicePrincipal('codepipeline.amazonaws.com'),
    });

    pipelineRole.addToPolicy(new iam.PolicyStatement({
      actions: [
        'secretsmanager:GetSecretValue',
      ],
      resources: [
        `arn:aws:secretsmanager:${cdk.Aws.REGION}:${cdk.Aws.ACCOUNT_ID}:secret:AWS_GITHUB_TOKEN-*`,
      ],
    }));

    // Definir a ação de origem
    const sourceAction = new codepipeline_actions.GitHubSourceAction({
      actionName: 'GitHub_Source',
      owner: 'matheusgonzalez91',
      repo: 'prontuario-cdk',
      oauthToken: cdk.SecretValue.secretsManager('AWS_GITHUB_TOKEN'),
      output: sourceOutput,
      branch: 'main',
    });

    // Definir o projeto de build do CodeBuild para o CDK
    const cdkBuildProject = new codebuild.PipelineProject(this, 'CdkBuildProject', {
      buildSpec: codebuild.BuildSpec.fromObject({
        version: '0.2',
        phases: {
          install: {
            commands: [
              'npm install -g aws-cdk',
              'npm install',
            ],
          },
          build: {
            commands: [
              'npx cdk synth',
            ],
          },
        },
        artifacts: {
          'base-directory': 'cdk.out',
          files: [
            'MyAthenaUserStack.template.json',
          ],
        },
      }),
      environment: {
        buildImage: codebuild.LinuxBuildImage.STANDARD_5_0,
      },
    });

    // Definir o projeto de build do CodeBuild para os scripts do Glue
    const glueScriptBuildProject = new codebuild.PipelineProject(this, 'GlueScriptBuildProject', {
      buildSpec: codebuild.BuildSpec.fromObject({
        version: '0.2',
        phases: {
          install: {
            commands: [
              'pip install awscli', // Instalar AWS CLI para fazer o upload dos scripts
            ],
          },
          build: {
            commands: [
              'aws s3 cp my_glue_jobs/ s3://myprontuariobucket/results/ --recursive', // Upload dos scripts para o bucket S3
            ],
          },
        },
        artifacts: {
          'base-directory': 'my_glue_jobs',
          files: [
            '**/*',
          ],
        },
      }),
      environment: {
        buildImage: codebuild.LinuxBuildImage.STANDARD_5_0,
      },
    });

    // Definir a ação de build para o CDK
    const buildAction = new codepipeline_actions.CodeBuildAction({
      actionName: 'CDK_Build',
      project: cdkBuildProject,
      input: sourceOutput,
      outputs: [cdkBuildOutput],
    });

    // Definir a ação de build para os scripts do Glue
    const glueScriptBuildAction = new codepipeline_actions.CodeBuildAction({
      actionName: 'Glue_Script_Build',
      project: glueScriptBuildProject,
      input: sourceOutput,
      outputs: [glueScriptOutput],
    });

    // Definir a ação de deploy do CloudFormation
    const deployAction = new codepipeline_actions.CloudFormationCreateUpdateStackAction({
      actionName: 'CFN_Deploy',
      stackName: 'MyAthenaUserStack',
      templatePath: cdkBuildOutput.atPath('MyAthenaUserStack.template.json'),
      adminPermissions: true,
    });

    // Definir o projeto de build para iniciar o Glue Job
    const glueJobStartProject = new codebuild.PipelineProject(this, 'GlueJobStartProject', {
      buildSpec: codebuild.BuildSpec.fromObject({
        version: '0.2',
        phases: {
          build: {
            commands: [
              'aws glue start-job-run --job-name job_prontuario'
            ],
          },
        },
      }),
      environment: {
        buildImage: codebuild.LinuxBuildImage.STANDARD_5_0,
      },
    });

    // Definir a ação para iniciar o Glue Job
    const glueJobStartAction = new codepipeline_actions.CodeBuildAction({
      actionName: 'Start_Glue_Job',
      project: glueJobStartProject,
      input: glueScriptOutput,
    });

    // Definir o pipeline
    new codepipeline.Pipeline(this, 'Pipeline', {
      pipelineName: 'MyCdkPipeline',
      artifactBucket: artifactBucket,
      role: pipelineRole,
      stages: [
        {
          stageName: 'Source',
          actions: [sourceAction],
        },
        {
          stageName: 'Build',
          actions: [buildAction, glueScriptBuildAction],
        },
        {
          stageName: 'Deploy',
          actions: [deployAction],
        },
        {
          stageName: 'GlueJob',
          actions: [glueJobStartAction],
        },
      ],
    });
  }
}
