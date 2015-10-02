// ModalaAMQ.cpp : Defines the entry point for the console application.
//

#include "stdafx.h"

#include <activemq/library/ActiveMQCPP.h>
#include <decaf/lang/Thread.h>
#include <decaf/lang/Runnable.h>
#include <decaf/lang/Integer.h>
#include <decaf/lang/Long.h>
#include <decaf/lang/System.h>
#include <activemq/core/ActiveMQConnectionFactory.h>
#include <activemq/util/Config.h>
#include <cms/Connection.h>
#include <cms/Session.h>
#include <cms/TextMessage.h>
#include <cms/BytesMessage.h>
#include <cms/MapMessage.h>
#include <cms/ExceptionListener.h>
#include <cms/MessageListener.h>

#include <stdlib.h>
#include <stdio.h>
#include <iostream>
#include <memory>

using namespace activemq::core;
using namespace decaf::util::concurrent;
using namespace decaf::util;
using namespace decaf::lang;
using namespace cms;
using namespace std;

class producer {
private:

	Connection* connection;
	Session* session;
	Destination* destination;
	MessageProducer* mproducer;
	int numMessages;
	bool sessionTransacted;
	std::string brokerURI;

private:

	producer(const producer&);
	producer& operator=(const producer&);

public:

	producer(const std::string& brokerURI, bool sessionTransacted = false) :
		connection(NULL),
		session(NULL),
		destination(NULL),
		mproducer(NULL),
		sessionTransacted(sessionTransacted),
		brokerURI(brokerURI) {
	}

	virtual ~producer(){
		cleanup();
	}

	void close() {
		this->cleanup();
	}

  void setup() {
		try {

			// Create a ConnectionFactory
			auto_ptr<ConnectionFactory> connectionFactory(
				ConnectionFactory::createCMSConnectionFactory(brokerURI));

			// Create a Connection
			connection = connectionFactory->createConnection();
			connection->start();

			// Create a Session
			if (this->sessionTransacted) {
				session = connection->createSession(Session::SESSION_TRANSACTED);
			}
			else {
				session = connection->createSession(Session::AUTO_ACKNOWLEDGE);
			}

			// Create the destination (Topic or Queue)
      destination = session->createQueue("MODALA.RESPONSES");

			// Create a MessageProducer from the Session to the Topic or Queue
			mproducer = session->createProducer(destination);
			mproducer->setDeliveryMode(DeliveryMode::NON_PERSISTENT);
    } catch (CMSException& e) {
			e.printStackTrace();
		}
  }

  void send(const std::string& text) {
    try {
      std::auto_ptr<TextMessage> message(session->createTextMessage(text));
      mproducer->send(message.get());
    }	catch (CMSException& e) {
			e.printStackTrace();
		}
  }

private:

	void cleanup() {

		if (connection != NULL) {
			try {
				connection->close();
			}
			catch (cms::CMSException& ex) {
				ex.printStackTrace();
			}
		}

		// Destroy resources.
		try {
			delete destination;
			destination = NULL;
			delete mproducer;
			mproducer = NULL;
			delete session;
			session = NULL;
			delete connection;
			connection = NULL;
		}
		catch (CMSException& e) {
			e.printStackTrace();
		}
	}
};

class consumer : public ExceptionListener,
	public MessageListener,
	public Runnable {

private:

	Connection* connection;
	Session* session;
	Destination* destination;
	MessageConsumer* mconsumer;
  producer *prod;
	bool useTopic;
	bool sessionTransacted;
	std::string brokerURI;

private:

	consumer(const consumer&);
	consumer& operator=(const consumer&);

public:

	consumer(const std::string& brokerURI, producer* p, bool sessionTransacted = false) :
		connection(NULL),
		session(NULL),
		destination(NULL),
		mconsumer(NULL),
    prod(p),
		sessionTransacted(sessionTransacted),
		brokerURI(brokerURI) {
	}

	virtual ~consumer() {
		cleanup();
	}

	void close() {
		this->cleanup();
	}

	virtual void run() {

		try {

			// Create a ConnectionFactory
			auto_ptr<ConnectionFactory> connectionFactory(
				ConnectionFactory::createCMSConnectionFactory(brokerURI));

			// Create a Connection
			connection = connectionFactory->createConnection();
			connection->start();
			connection->setExceptionListener(this);

			// Create a Session
			if (this->sessionTransacted == true) {
				session = connection->createSession(Session::SESSION_TRANSACTED);
			}
			else {
				session = connection->createSession(Session::AUTO_ACKNOWLEDGE);
			}

      destination = session->createQueue("MODALA.QUERIES");

			// Create a MessageConsumer from the Session to the Topic or Queue
			mconsumer = session->createConsumer(destination);

			mconsumer->setMessageListener(this);

			std::cout.flush();
			std::cerr.flush();

      while(true) {
        // just listen for messages
        Sleep(100);
      }

		}
		catch (CMSException& e) {
			e.printStackTrace();
		}
	}

	// Called from the consumer since this class is a registered MessageListener.
	virtual void onMessage(const Message* message) {
		try {
			const TextMessage* textMessage = dynamic_cast<const TextMessage*> (message);

			if (textMessage != NULL) {
        string text = "";
				text = textMessage->getText();
        printf("Request received: %s\n", text.c_str());


        // THIS IS WHERE THE OPTIMIZER SHOULD BE CALLED

        // 1. Parse string as json. Check request type and input data
        // 2. Start optimizer (give it the producer so that it can output progress messages)
        // 3. Have optimizer send progress messages every n:th iteration, or second, or something
        // 4. Create json containing final result. Have the producer send the result again.

        for(int i = 0; i < 10; ++i) {
          prod->send("progress");
          Sleep(1000);
        }
        prod->send(string("reply: ") + text);
			}
		}
		catch (CMSException& e) {
			e.printStackTrace();
		}

		// Commit all messages.
		if (this->sessionTransacted) {
			session->commit();
		}
	}

	// If something bad happens you see it here as this class is also been
	// registered as an ExceptionListener with the connection.
	virtual void onException(const CMSException& ex AMQCPP_UNUSED) {
		printf("CMS Exception occurred.  Shutting down client.\n");
		ex.printStackTrace();
		exit(1);
	}

private:

	void cleanup() {
		if (connection != NULL) {
			try {
				connection->close();
			}
			catch (cms::CMSException& ex) {
				ex.printStackTrace();
			}
		}

		// Destroy resources.
		try {
			delete destination;
			destination = NULL;
			delete mconsumer;
			mconsumer = NULL;
			delete session;
			session = NULL;
			delete connection;
			connection = NULL;
		}
		catch (CMSException& e) {
			e.printStackTrace();
		}
	}
};

int _tmain(int argc, _TCHAR* argv[]) {
  // setup
	activemq::library::ActiveMQCPP::initializeLibrary();
  std::string brokerURI = "tcp://localhost:61616";
  producer producer(brokerURI);
  producer.setup();
  consumer consumer(brokerURI, &producer);

  Thread consumerThread(&consumer);
  consumerThread.start();

  consumerThread.join();  // will never reach here

  // cleanup
  consumer.close();
  producer.close();
	activemq::library::ActiveMQCPP::shutdownLibrary();
	return 0;
}
