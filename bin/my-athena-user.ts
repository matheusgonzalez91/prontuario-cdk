import * as cdk from 'aws-cdk-lib';
import { MyAthenaUserStack } from '../lib/my-athena-user-stack';
import { MyPipelineStack } from '../lib/my-pipeline-stack';

const app = new cdk.App();
new MyAthenaUserStack(app, 'MyAthenaUserStack');
new MyPipelineStack(app, 'MyPipelineStack');

app.synth();
