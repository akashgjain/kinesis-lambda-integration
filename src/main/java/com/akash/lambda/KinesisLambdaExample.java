package com.akash.lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.KinesisEvent;
import com.amazonaws.services.lambda.runtime.events.KinesisEvent.KinesisEventRecord;

public class KinesisLambdaExample implements RequestHandler<KinesisEvent, String> {

	double sumProfit = 0.0;

	public String handleRequest(KinesisEvent arg0, Context arg1) {
		// TODO Auto-generated method stub
		for (KinesisEventRecord rec : arg0.getRecords()) {
			String payload = new String(rec.getKinesis().getData().array());
			System.out.println("Decoded Payload : " + payload);

			String line[] = payload.split(",");
			double netProfit = Double.parseDouble(line[2]) - Double.parseDouble(line[3]);
			sumProfit += netProfit;

		}
		System.out.println("Net Profit " + sumProfit);
		return "Net Profit "  + sumProfit;

	}
}