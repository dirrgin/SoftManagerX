import logging, os
import azure.functions as func
from azure.communication.email import EmailClient

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)


@app.route(route="IoT_Postman")
def IoT_Postman(req: func.HttpRequest) -> func.HttpResponse:
    try:
        connection_string = os.getenv('EMAIL_SRV_CONNECTION_STRING')
        if not connection_string:
            raise ValueError("Connection string is undefined.")

        req_body = req.get_json()
        if not req_body or not isinstance(req_body, list) or len(req_body) == 0:
            logging.info("Received an empty or invalid batch.")
            return func.HttpResponse("No data to process.", status_code=200)

        email_body_parts = []
        email_body_details=[]
        for item in req_body:
            device_name = item.get("DeviceName", "Unknown Device")
            device_error = item.get("DeviceError", "Unrecognised type of the error")
            workorderId = item.get("WorkorderId", "Unknown ID")
            eventEnqueuedUtcTime = item.get("EventEnqueuedUtcTime", "Unrecognised time")

            email_body_parts.append(f"{device_name} reported an error: {device_error}.")
            email_body_details.append(f"Further details:\n\tWith workorder ID: {workorderId} at {eventEnqueuedUtcTime}")
            body = "\n".join(email_body_parts)
            details = "\n".join(email_body_details)
            recipient_email = 'ul0278786@edu.uni.lodz.pl'
            try: 
                email_client = EmailClient.from_connection_string(connection_string)
            except Exception as conn:
                return func.HttpResponse(f"Failed to send email. Error: {str(conn)}", status_code=503) 
            message = {
                "senderAddress": "DoNotReply@1c12bc12-fb37-4240-a238-044677101d2e.azurecomm.net",
                "recipients":  {
                    "to": [{"address": recipient_email }]
                },
                "content": {
                    "subject": "IoT Device Error Report",
                    "plainText": body,
                    "html": f"<html><h3>{body}</h3></html></h3><small><em>{details}</em></small></html>"
                }
            }
            try:
                poller = email_client.begin_send(message)
                result = poller.result()
                logging.info(f"Email sent successfully. Status: {result['status']}")
                return func.HttpResponse(f"Email sent successfully", status_code=200)
            except Exception as e:
                logging.error(f"Email_Client Failed to send email. Error: {str(e)}", exc_info=True)
        return func.HttpResponse(f"Failed to send email. Error: {str(e)}", status_code=502)  # Change status code to 502 (Bad Gateway)
        
    
    except ValueError as ve:
        logging.error(f"Invalid JSON: {str(ve)}")
        return func.HttpResponse("Invalid JSON data :( ", status_code=400)
    
    except Exception as e:
        logging.error(f"Failed to send email. Error: {str(e)}")
        return func.HttpResponse(f"Failed to send email. Error: {str(e)}", status_code=500)
    
