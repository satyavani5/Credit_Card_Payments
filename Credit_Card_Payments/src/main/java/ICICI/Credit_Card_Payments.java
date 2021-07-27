package ICICI;

import java.net.URI;

import micronet.activemq.AMQGatewayPeer;
import micronet.annotation.MessageListener;
import micronet.annotation.MessageParameter;
import micronet.annotation.MessageService;
import micronet.annotation.OnStart;
import micronet.annotation.OnStop;
import micronet.annotation.RequestParameters;
import micronet.network.Context;
import micronet.network.NetworkConstants;
import micronet.network.Request;
import micronet.network.Response;
import micronet.network.StatusCode;
import micronet.serialization.Serialization;

@MessageService(uri="mn://gateway")
public class Credit_Card_Payments {

	private AMQGatewayPeer gatewayPeer;
	
	private ConnectionStore connections = new ConnectionStore();

	@OnStart
	public void onStart(Context context) {
		gatewayPeer = new AMQGatewayPeer((String connectionID) -> {
			UserConnection connection = connections.get(connectionID);
			if (connection == null)
				return;
			connections.remove(connectionID);
            context.getAdvisory().send("User.Disconnected", Integer.toString(connection.getUserID()));

		});
		gatewayPeer.listen(URI.create(NetworkConstants.COMMAND_QUEUE), (String clientId, Request request) -> clientCmd(context, clientId, request));
		gatewayPeer.listen(URI.create(NetworkConstants.REQUEST_QUEUE), (String clientId, Request request) -> clientRequest(context, clientId, request));
	}
	
	@OnStop
	public void onStop(Context context) {
		
	}
	
	@MessageListener(uri="/forward/event")
	@RequestParameters(@MessageParameter(code=ParameterCode.USER_ID, type=Integer.class))
	public void forwardEvent(Context context, Request request) {
		// TODO: Dont send back userId (security)
		int userID = request.getParameters().getInt(ParameterCode.USER_ID);
		UserConnection connection = connections.get(userID);
		gatewayPeer.sendRequest(URI.create(connections.getConnectionURI(connection) + "/event"), request);
	}
	
	@MessageListener(uri="/broadcast/event")
	public void broadcastEvent(Context context, Request request) {
		for (UserConnection connection : connections.all()) {
			gatewayPeer.sendRequest(URI.create(connections.getConnectionURI(connection) + "/event"), request);
		}
	}

	private void clientCmd(Context context, String connectionID, Request request) {
		UserConnection connection = connections.get(connectionID);
		if (connection == null)
			return;
		String userRequest = request.getParameters().getString(ParameterCode.USER_REQUEST);
		System.out.println("CMD " + connectionID + " -> " + userRequest + ": " + request.getData());
		Request forwardRequest = new Request(request.getData());
		forwardRequest.getParameters().set(ParameterCode.USER_ID, connection.getUserID());
		context.sendRequest(userRequest, forwardRequest);
	}

	private Response clientRequest(Context context, String connectionID, Request request) {

		String userRequest = request.getParameters().getString(ParameterCode.USER_REQUEST);
		System.out.println("REQUEST " + connectionID + " -> " + userRequest + ": " + request.getData());
		
		UserConnection connection = connections.get(connectionID);

		switch (userRequest) {
		case "mn://account/register":
			return context.sendRequestBlocking(userRequest, request);
		case "mn://logout/":
			if (connection == null)
				return new Response(StatusCode.FORBIDDEN, "You are not logged in");
			connections.remove(connectionID);
			return new Response(StatusCode.OK, "Logged Out");
		case "mn://account/login":
			if (connection != null)
				return new Response(StatusCode.FORBIDDEN, "Already logged in");
			Response loginResponse = context.sendRequestBlocking(userRequest, request);
			if (loginResponse.getStatus() == StatusCode.OK) {
				int userID = Serialization.deserialize(loginResponse.getData(), Integer.class);
				connection = connections.get(userID);
				if (connection != null)
					connections.remove(connection.getConnectionID());
					//return new Response(StatusCode.FORBIDDEN, "User ID already in use");
				connection = connections.add(connectionID, userID);
				return new Response(StatusCode.OK);
			}
			return loginResponse;
		default:
			if (connection == null)
				return new Response(StatusCode.UNAUTHORIZED, "Not Authenticated: Only register and login possible");
			
			//TODO: Add white- or blacklisting here
			
			request.getParameters().set(ParameterCode.USER_ID, connection.getUserID());
			return context.sendRequestBlocking(userRequest, request);
		}
	}
}
