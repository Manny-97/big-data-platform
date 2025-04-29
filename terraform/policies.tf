#step function role permissions
data "aws_iam_policy_document" "stfn_trust_policy" {
  statement {
    effect = "Allow"

    principals {
      type        = "Service"
      identifiers = ["states.amazonaws.com"]
    }

    actions = ["sts:AssumeRole"]
  }
}


data "aws_iam_policy_document" "stfn_policy" {
  statement {
    effect    = "Allow"
    actions   = [
                "events:PutTargets",
                "events:PutRule",
                "events:DescribeRule",
                "emr-serverless:StartJobRun",
                "iam:PassRole",
                "logs:*",
                "cloudwatch:*"
      ]
    resources = [
                "arn:aws:events:us-east-1:181438960030:rule/StepFunctionsGetEventsForEMRServerlessApplicationRule",
                "arn:aws:emr-serverless:us-east-1:181438960030:/applications/*",
                "*"
    ]
  }
  statement {
    effect    = "Allow"
    actions = [
                "emr-serverless:*",
                "s3:*"
                ]
    resources = ["*"]
  }
}


#emr role permissions
data "aws_iam_policy_document" "emr_serverless_trust_policy" {
  statement {
    effect = "Allow"

    principals {
      type        = "Service"
      identifiers = ["emr-serverless.amazonaws.com"]
    }

    actions = ["sts:AssumeRole"]
  }
}


data "aws_iam_policy_document" "emr_serverless_policy"{
  statement {
    effect = "Allow"
    actions =  [
      "logs:*",
      "s3:*"
    ]
    resources = [
      "*"
    ]
  }
}


# IAM policy to allow EventBridge to invoke Step Functions state machine
data "aws_iam_policy_document" "eventbridge_trust_policy" {
  statement {
    effect = "Allow"

    principals {
      type        = "Service"
      identifiers = ["events.amazonaws.com"]
    }

    actions = ["sts:AssumeRole"]
  }
}


data "aws_iam_policy_document" "eventbridge_invoke_stfn_policy" {
  statement {
    effect    = "Allow"
    actions   = ["states:StartExecution"]
    resources = [aws_sfn_state_machine.emr_sfn_state_machine.arn]
  }
}
