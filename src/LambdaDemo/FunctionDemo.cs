using Amazon.Lambda.APIGatewayEvents;
using Amazon.Lambda.Core;
using Amazon.SQS;
using Amazon.SQS.Model;
using LambdaDemo.Model;
using System.Net;
using System.Text.Json;

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.SystemTextJson.DefaultLambdaJsonSerializer))]

namespace LambdaDemo;

public class FunctionDemo
{
    private static readonly string _queueUrl = "https://sqs.us-east-2.amazonaws.com/025381531841/sqs-demo";

    /// <summary>
    /// API to send contacts to SQS queue 
    /// </summary>
    /// <param name="request"></param>
    /// <param name="context"></param>
    /// <returns></returns>
    public static async Task<APIGatewayProxyResponse> Post(APIGatewayProxyRequest request, ILambdaContext context)
    {
        var statusCode = (int)HttpStatusCode.OK;

        string? responseMessage;

        try
        {
            var contacts = GetContactsFromRequest(request);

            await SendContactsToQueue(contacts);

            responseMessage = $"Total contacts sent to SQS: {(contacts != null ? contacts.Count : default)}";
        }
        catch (Exception ex)
        {
            statusCode = (int)HttpStatusCode.InternalServerError;

            responseMessage = $"Error: {ex.Message}"; 
        }

        context.Logger.Log(responseMessage);

        return new APIGatewayProxyResponse
        {
            StatusCode = statusCode,
            Body = responseMessage
        };
    }

    private static async Task SendContactsToQueue(List<Contact>? contacts)
    {
        if (contacts == null)
            return;

        var sqsClient = new AmazonSQSClient();

        foreach (var contact in contacts)
        {
            var message = new SendMessageRequest
            {
                QueueUrl = _queueUrl,
                MessageBody = JsonSerializer.Serialize(contact)
            };

            await sqsClient.SendMessageAsync(message);
        }
    }

    private static List<Contact>? GetContactsFromRequest(APIGatewayProxyRequest request)
    {
        var options = new JsonSerializerOptions { PropertyNameCaseInsensitive = true };

        return JsonSerializer.Deserialize<List<Contact>>(request.Body, options);
    }
}