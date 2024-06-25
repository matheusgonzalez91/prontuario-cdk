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

    // Definir as variáveis de entrada do pipeline
    const sourceOutput = new codepipeline.Artifact();
    const cdkBuildOutput = new codepipeline.Artifact();

    // Definir a ação de origem (por exemplo, GitHub)
    const sourceAction = new codepipeline_actions.GitHubSourceAction({
      actionName: 'GitHub_Source',
      owner: 'matheusgonzalez91',
      repo: 'prontuario-cdk',
      oauthToken: cdk.SecretValue.secretsManager('GITHUB_TOKEN'),
      output: sourceOutput,
      branch: 'main',
    });

    // Definir o projeto de build do CodeBuild
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

    // Definir a ação de build
    const buildAction = new codepipeline_actions.CodeBuildAction({
      actionName: 'CDK_Build',
      project: cdkBuildProject,
      input: sourceOutput,
      outputs: [cdkBuildOutput],
    });

    // Definir a ação de deploy do CloudFormation
    const deployAction = new codepipeline_actions.CloudFormationCreateUpdateStackAction({
      actionName: 'CFN_Deploy',
      stackName: 'MyAthenaUserStack',
      templatePath: cdkBuildOutput.atPath('MyAthenaUserStack.template.json'),
      adminPermissions: true,
    });

    // Definir o pipeline
    new codepipeline.Pipeline(this, 'Pipeline', {
      pipelineName: 'MyCdkPipeline',
      artifactBucket: artifactBucket,
      stages: [
        {
          stageName: 'Source',
          actions: [sourceAction],
        },
        {
          stageName: 'Build',
          actions: [buildAction],
        },
        {
          stageName: 'Deploy',
          actions: [deployAction],
        },
      ],
    });
  }
}
