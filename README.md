# Lambda-Function for API Monitoring
Automated CRON job to test the [HuggingFace API](https://huggingface.co/spaces/a13awd/electricity_grid_model) every 12 hours using AWS EventBridge.

## Overview

This service runs on a scheduled basis using AWS EventBridge to ensure that the deployed prediction API remains operational and responsive.

## Features

- Automated API health checks every 12 hours
- Integration with Hugging Face prediction endpoint
- Event-driven execution using AWS EventBridge
- Lightweight serverless architecture

## Workflow

1. EventBridge triggers Lambda on schedule
2. Lambda sends request to prediction API
3. Response is validated
4. Failures can be logged or monitored

## Purpose

- Ensure API reliability
- Detect downtime or failures early
- Support production readiness

## Deployment

- Hosted on AWS Lambda
- Triggered via EventBridge (CRON schedule)

## Example Use Case

- Continuous monitoring of ML inference services
- Reliability layer for external API dependencies

## Notes

This service can be extended to include:
- Alerting (SNS, email)
- Logging dashboards
- Retry mechanisms
