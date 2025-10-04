"""
Utility functions for sending notifications (email, Slack, etc.) on task failures and success
"""
import http
from airflow.utils.email import send_email
from airflow.models import Variable
from datetime import datetime
from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook
from sqlalchemy.sql.coercions import expect



def send_slack_message(message: str, color: str = "#36a64f"):
    """
    Send message to Slack using SlackWebhookHook.
    Uses Airflow Connection 'slack_webhook' (created via Airflow UI or CLI).
    Compatible with apache-airflow-providers-slack >= 8.0.0
    """
    try:
        # Create SlackWebhookHook using the connection ID
        # The connection should be set up in Airflow UI: Admin -> Connections
        # Connection ID: slack_webhook
        # Connection Type: HTTP
        # Host: https://hooks.slack.com/services
        # Password: YOUR_WEBHOOK_TOKEN (e.g., T09JHVB4FPV/B09KJKSFX6C/...)
        hook = SlackWebhookHook(
            slack_webhook_conn_id="slack_webhook"  # Connection ID we created
        )
        
        # Send the message using the .send() method
        hook.send(
            text=message,
            username="Airflow Bot",
            icon_emoji=":robot_face:"
        )
        
        print(f"✅ Slack notification sent successfully!")
        print(f"   Message: {message[:70]}...")
        
    except Exception as e:
        print(f"❌ Failed to send Slack notification: {str(e)}")
        import traceback
        traceback.print_exc()

def send_slack_failure(context):
    #Send Slack Notification when task fails
    task_instance=context.get('task_instance')
    dag_id=context.get('dag').dag_id
    task_id=task_instance.task_id
    execution_date=context.get('execution_date')
    log_url=task_instance.log_url
    exception=context.get('exception')

    message = (
        f":rotating_light: *Airflow Task Failed!* :rotating_light:\n"
        f"*DAG:* `{dag_id}`\n"
        f"*Task:* `{task_id}`\n"
        f"*Execution Date:* {execution_date}\n"
        f"*Error:* `{exception}`\n"
        f"<{log_url}|📋 View Logs>"
    )

    send_slack_message(message,color="#d32f2f")

def send_slack_success(context):
    task_instance=context.get('task_instance')  # Fixed typo: task_instance not task_intance
    dag_id=context.get('dag').dag_id
    task_id=task_instance.task_id
    execution_date=context.get('execution_date')
    duration=task_instance.duration

    message = (
        f":white_check_mark: *Airflow Task Succeeded!* :white_check_mark:\n"
        f"*DAG:* `{dag_id}`\n"
        f"*Task:* `{task_id}`\n"
        f"*Execution Date:* {execution_date}\n"
        f"*Duration:* {duration:.2f} seconds"
    )
    send_slack_message(message,"#36a64f")

def send_slack_retry(context):
    """
    Send Slack notification when task is retrying.
    """
    task_instance = context.get("task_instance")
    dag_id = context.get("dag").dag_id
    task_id = task_instance.task_id
    execution_date = context.get("execution_date")
    try_number = task_instance.try_number
    max_tries = task_instance.max_tries
    exception = context.get("exception")

    message = (
        f":repeat: *Airflow Task Retrying...*\n"
        f"*DAG:* `{dag_id}`\n"
        f"*Task:* `{task_id}`\n"
        f"*Execution Date:* {execution_date}\n"
        f"*Attempt:* {try_number}/{max_tries}\n"
        f"*Error:* `{exception}`"
    )

    send_slack_message(message, color="#ff9800")



def send_failure_email(context):
    """
    Send email notification when a task fails.
    
    Usage in DAG:
        default_args = {
            'on_failure_callback': send_failure_email,
        }
    
    Configure email recipients via Airflow Variable:
        Variable.set("failure_notification_emails", "user1@example.com,user2@example.com")
    """
    task_instance = context.get('task_instance')
    dag_id = context.get('dag').dag_id
    task_id = task_instance.task_id
    execution_date = context.get('execution_date')
    log_url = task_instance.log_url
    exception = context.get('exception')
    
    subject = f"🚨 Airflow Task Failed: {dag_id}.{task_id}"
    
    html_content = f"""
    <html>
    <body>
        <h2 style="color: #d32f2f;">⚠️ Task Failure Alert</h2>
        <table style="border-collapse: collapse; width: 100%; max-width: 600px;">
            <tr style="background-color: #f5f5f5;">
                <td style="padding: 10px; border: 1px solid #ddd;"><strong>DAG</strong></td>
                <td style="padding: 10px; border: 1px solid #ddd;">{dag_id}</td>
            </tr>
            <tr>
                <td style="padding: 10px; border: 1px solid #ddd;"><strong>Task</strong></td>
                <td style="padding: 10px; border: 1px solid #ddd;">{task_id}</td>
            </tr>
            <tr style="background-color: #f5f5f5;">
                <td style="padding: 10px; border: 1px solid #ddd;"><strong>Execution Date</strong></td>
                <td style="padding: 10px; border: 1px solid #ddd;">{execution_date}</td>
            </tr>
            <tr>
                <td style="padding: 10px; border: 1px solid #ddd;"><strong>Status</strong></td>
                <td style="padding: 10px; border: 1px solid #ddd; color: #d32f2f;"><strong>FAILED</strong></td>
            </tr>
            <tr style="background-color: #ffebee;">
                <td style="padding: 10px; border: 1px solid #ddd;"><strong>Error</strong></td>
                <td style="padding: 10px; border: 1px solid #ddd; font-family: monospace; font-size: 12px;">{exception}</td>
            </tr>
        </table>
        <br>
        <a href="{log_url}" style="background-color: #1976d2; color: white; padding: 10px 20px; text-decoration: none; border-radius: 4px;">
            📋 View Logs
        </a>
        <hr style="margin-top: 30px; border: none; border-top: 1px solid #ddd;">
        <p style="color: #666; font-size: 12px;">
            This is an automated alert from the Airflow ETL Pipeline.<br>
            Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
        </p>
    </body>
    </html>
    """
    
    # Get email recipients from Airflow Variables (fallback to default)
    try:
        to_emails = Variable.get("failure_notification_emails", default_var="admin@example.com").split(",")
        to_emails = [email.strip() for email in to_emails]  # Clean whitespace
    except Exception as e:
        print(f"Error getting email variable: {str(e)}")
        to_emails = ["admin@example.com"]
    
    try:
        send_email(to=to_emails, subject=subject, html_content=html_content)
        print(f"✅ Failure notification sent to: {', '.join(to_emails)}")
    except Exception as e:
        print(f"❌ Failed to send email notification: {str(e)}")
        # Log but don't fail the DAG on email failure


def send_success_email(context):
    """
    Send email notification when a task succeeds.
    
    Usage in DAG:
        default_args = {
            'on_success_callback': send_success_email,
        }
    """
    task_instance = context.get('task_instance')
    dag_id = context.get('dag').dag_id
    task_id = task_instance.task_id
    execution_date = context.get('execution_date')
    duration = task_instance.duration
    
    subject = f"✅ Airflow Task Succeeded: {dag_id}.{task_id}"
    
    html_content = f"""
    <html>
    <body>
        <h2 style="color: #388e3c;">✅ Task Success</h2>
        <table style="border-collapse: collapse; width: 100%; max-width: 600px;">
            <tr style="background-color: #f5f5f5;">
                <td style="padding: 10px; border: 1px solid #ddd;"><strong>DAG</strong></td>
                <td style="padding: 10px; border: 1px solid #ddd;">{dag_id}</td>
            </tr>
            <tr>
                <td style="padding: 10px; border: 1px solid #ddd;"><strong>Task</strong></td>
                <td style="padding: 10px; border: 1px solid #ddd;">{task_id}</td>
            </tr>
            <tr style="background-color: #f5f5f5;">
                <td style="padding: 10px; border: 1px solid #ddd;"><strong>Execution Date</strong></td>
                <td style="padding: 10px; border: 1px solid #ddd;">{execution_date}</td>
            </tr>
            <tr>
                <td style="padding: 10px; border: 1px solid #ddd;"><strong>Status</strong></td>
                <td style="padding: 10px; border: 1px solid #ddd; color: #388e3c;"><strong>SUCCESS</strong></td>
            </tr>
            <tr style="background-color: #f5f5f5;">
                <td style="padding: 10px; border: 1px solid #ddd;"><strong>Duration</strong></td>
                <td style="padding: 10px; border: 1px solid #ddd;">{duration:.2f} seconds</td>
            </tr>
        </table>
        <hr style="margin-top: 30px; border: none; border-top: 1px solid #ddd;">
        <p style="color: #666; font-size: 12px;">
            This is an automated notification from the Airflow ETL Pipeline.<br>
            Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
        </p>
    </body>
    </html>
    """
    
    try:
        to_emails = Variable.get("success_notification_emails", default_var="").split(",")
        to_emails = [email.strip() for email in to_emails if email.strip()]
        
        if to_emails:
            send_email(to=to_emails, subject=subject, html_content=html_content)
            print(f"✅ Success notification sent to: {', '.join(to_emails)}")
    except Exception as e:
        print(f"⚠️ Success notification skipped or failed: {str(e)}")


def send_retry_email(context):
    """
    Send email notification when a task is retrying.
    
    Usage in DAG:
        default_args = {
            'on_retry_callback': send_retry_email,
        }
    """
    task_instance = context.get('task_instance')
    dag_id = context.get('dag').dag_id
    task_id = task_instance.task_id
    execution_date = context.get('execution_date')
    try_number = task_instance.try_number
    max_tries = task_instance.max_tries
    exception = context.get('exception')
    
    subject = f"🔄 Airflow Task Retrying: {dag_id}.{task_id} (Attempt {try_number}/{max_tries})"
    
    html_content = f"""
    <html>
    <body>
        <h2 style="color: #ff9800;">🔄 Task Retry Alert</h2>
        <table style="border-collapse: collapse; width: 100%; max-width: 600px;">
            <tr style="background-color: #f5f5f5;">
                <td style="padding: 10px; border: 1px solid #ddd;"><strong>DAG</strong></td>
                <td style="padding: 10px; border: 1px solid #ddd;">{dag_id}</td>
            </tr>
            <tr>
                <td style="padding: 10px; border: 1px solid #ddd;"><strong>Task</strong></td>
                <td style="padding: 10px; border: 1px solid #ddd;">{task_id}</td>
            </tr>
            <tr style="background-color: #f5f5f5;">
                <td style="padding: 10px; border: 1px solid #ddd;"><strong>Execution Date</strong></td>
                <td style="padding: 10px; border: 1px solid #ddd;">{execution_date}</td>
            </tr>
            <tr>
                <td style="padding: 10px; border: 1px solid #ddd;"><strong>Status</strong></td>
                <td style="padding: 10px; border: 1px solid #ddd; color: #ff9800;"><strong>RETRYING</strong></td>
            </tr>
            <tr style="background-color: #f5f5f5;">
                <td style="padding: 10px; border: 1px solid #ddd;"><strong>Attempt</strong></td>
                <td style="padding: 10px; border: 1px solid #ddd;">{try_number} of {max_tries}</td>
            </tr>
            <tr style="background-color: #fff3e0;">
                <td style="padding: 10px; border: 1px solid #ddd;"><strong>Error</strong></td>
                <td style="padding: 10px; border: 1px solid #ddd; font-family: monospace; font-size: 12px;">{exception}</td>
            </tr>
        </table>
        <hr style="margin-top: 30px; border: none; border-top: 1px solid #ddd;">
        <p style="color: #666; font-size: 12px;">
            This is an automated notification from the Airflow ETL Pipeline.<br>
            Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
        </p>
    </body>
    </html>
    """
    
    try:
        to_emails = Variable.get("retry_notification_emails", default_var="").split(",")
        to_emails = [email.strip() for email in to_emails if email.strip()]
        
        if to_emails:
            send_email(to=to_emails, subject=subject, html_content=html_content)
            print(f"🔄 Retry notification sent to: {', '.join(to_emails)}")
    except Exception as e:
        print(f"⚠️ Retry notification failed: {str(e)}")


# Wrapper functions for combined notifications
def notify_failure(context):
    """Send both email and Slack notifications on failure"""
    send_failure_email(context)
    send_slack_failure(context)

def notify_success(context):
    """Send both email and Slack notifications on success"""
    send_success_email(context)
    send_slack_success(context)

# Default args template for easy reuse
DEFAULT_NOTIFICATION_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'on_failure_callback': notify_failure,  # ✅ Calls both email and Slack
    # Uncomment to enable success notifications:
    # 'on_success_callback': notify_success,
    # Uncomment to enable retry notifications:
    # 'on_retry_callback': send_retry_email,
}

