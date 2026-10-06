variable "project_id" { type = string }
variable "region" {
  type    = string
  default = "us-central1"
}
variable "billing_account" {
  type        = string
  description = "Billing account ID for the budget alert (created FIRST)."
}
variable "budget_usd" {
  type    = number
  default = 5
}
