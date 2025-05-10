#step function role creation and policy attachment
resource "aws_iam_role" "step_func_role" {
  name = "step-function-role"
  assume_role_policy = data.aws_iam_policy_document.stfn_trust_policy.json
}


resource "aws_iam_role_policy" "step_function_policy_attachment" {
  role       = aws_iam_role.step_func_role.name
  policy = data.aws_iam_policy_document.stfn_policy.json
}


resource "aws_iam_policy_attachment" "aws_eventbridge_access" {
  name       = "AmazonEventBridgeFullAccess-attachment"
  policy_arn = "arn:aws:iam::aws:policy/AmazonEventBridgeFullAccess"
  roles  = [aws_iam_role.step_func_role.name]
}


resource "aws_iam_policy_attachment" "aws_stfn_full_access" {
  name       = "AWSStepFunctionsFullAccess-attachment"
  policy_arn = "arn:aws:iam::aws:policy/AWSStepFunctionsFullAccess"
  roles  = [aws_iam_role.step_func_role.name]
}


resource "aws_iam_policy_attachment" "aws_lambda_access" {
  name       = "AWSLambdarole-attachment"
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaRole"
  roles  = [aws_iam_role.step_func_role.name]
}


#emr execution role creation and policy attachment
resource "aws_iam_role" "emr_execution_role"{
name = "emr-execution-role"
assume_role_policy = data.aws_iam_policy_document.emr_serverless_trust_policy.json
}


resource "aws_iam_role_policy" "emr_execution_policy_attachment" {
  role       = aws_iam_role.emr_execution_role.name
  policy = data.aws_iam_policy_document.emr_serverless_policy.json
}


#step function
resource "aws_sfn_state_machine" "emr_sfn_state_machine" {
  name     = "emr-severless-state-machine"
  role_arn = aws_iam_role.step_func_role.arn
  definition = templatefile("${path.module}/stfn.asl.json",{
    EXECUTION_ROLE_ARN = aws_iam_role.emr_execution_role.arn
  })

  depends_on = [
aws_iam_role.step_func_role,
    aws_iam_role_policy.step_function_policy_attachment,
    aws_iam_policy_attachment.aws_eventbridge_access,
    aws_iam_policy_attachment.aws_stfn_full_access,
    aws_iam_policy_attachment.aws_lambda_access,
    aws_iam_role.emr_execution_role,
    aws_iam_role_policy.emr_execution_policy_attachment,
    aws_s3_bucket.company_emr_bucket,
    aws_s3_bucket.source_bucket,
    aws_s3_object.upload_file,
    aws_s3_bucket.cleaned_bucket
       ]
}
