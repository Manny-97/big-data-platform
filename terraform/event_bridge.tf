resource "aws_iam_role" "eventbridge_execution_role" {
  name               = "eventbridge-execution-role"
  assume_role_policy = data.aws_iam_policy_document.eventbridge_trust_policy.json
}


resource "aws_iam_role_policy" "eventbridge_invoke_stfn_policy_attachment" {
  name   = "eventbridge-invoke-stfn-policy-attachment"
  role   = aws_iam_role.eventbridge_execution_role.name
  policy = data.aws_iam_policy_document.eventbridge_invoke_stfn_policy.json
}


resource "aws_cloudwatch_event_rule" "schedule_emr_step_function" {
  name                = "ScheduleEMRServerlessStepFunction"
  description         = "This EventBridge rule schedules the execution of Step Functions to manage EMR serverless."
  schedule_expression = "cron(41 17 24 4 ? 2025)"

}


resource "aws_cloudwatch_event_target" "step_function_target" {
  rule      = aws_cloudwatch_event_rule.schedule_emr_step_function.name
  target_id = "StepFunctionTarget"
  arn       = aws_sfn_state_machine.emr_sfn_state_machine.arn
  role_arn  = aws_iam_role.eventbridge_execution_role.arn
  depends_on = [
    aws_sfn_state_machine.emr_sfn_state_machine,
    ]
}
