package artio;

import org.agrona.concurrent.Agent;
import org.agrona.concurrent.SleepingIdleStrategy;
import uk.co.real_logic.artio.Reply;
import uk.co.real_logic.artio.builder.HeaderEncoder;
import uk.co.real_logic.artio.builder.SessionHeaderEncoder;
import uk.co.real_logic.artio.builder.TestRequestEncoder;
import uk.co.real_logic.artio.library.FixLibrary;
import uk.co.real_logic.artio.library.SessionConfiguration;
import uk.co.real_logic.artio.messages.SessionState;
import uk.co.real_logic.artio.session.Session;
import uk.co.real_logic.artio.session.SessionWriter;

import java.time.Clock;
import java.time.Duration;

public class SessionAgent implements Agent {

    public static final int FRAGMENT_LIMIT = 10;
    private final FixLibrary library;
    private final SleepingIdleStrategy idleStrategy;
    private Session sessionQuickfix = null;

    private final Clock now = Clock.systemDefaultZone();
    private final SessionConfiguration quickfixSessionConfig;
    private long triggerTime;
    private TestRequestEncoder testRequestEncoder;
    private Reply<Session> reply;

    private State state = State.CONNECT;
    private Reply<SessionWriter> reply2;
    private SessionWriter sessionWriter;

    public SessionAgent(FixLibrary library, SleepingIdleStrategy idleStrategy) {
        this.library = library;
        this.idleStrategy = idleStrategy;
        quickfixSessionConfig = SessionConfiguration.builder()
                .address("localhost", 9999)
                .targetCompId("TEST_TARGET")
                .senderCompId("TEST_SENDER")
                .build();

    }

    @Override
    public void onStart() {
        testRequestEncoder = new TestRequestEncoder();
        testRequestEncoder.testReqID("Hello World");
    }

    @Override
    public int doWork() throws Exception {
        switch (state) {
            case CONNECT:
                reply = library.initiate(quickfixSessionConfig);
                followSessionPOC(library);
                state = State.WAITING_CONNECTION;
                break;
            case WAITING_CONNECTION:
                switch (reply.state()) {
                    case TIMED_OUT:
                    case ERRORED:
                        reply = null;
                        break;
                    case COMPLETED:
                        sessionQuickfix = reply.resultIfPresent();
                        state = State.WAITING_FOLLOWER;
                        break;
                }
                switch (reply2.state()) {
                    case TIMED_OUT:
                    case ERRORED:
                        reply2 = null;
                        System.out.println("RPA timeout ");
                        break;
                    case COMPLETED:
                        sessionWriter = reply2.resultIfPresent();
                        System.out.println("RPA success " + sessionWriter.id() + ";" + sessionWriter.connectionId());
                        state = State.CONNECTED;
                        break;
                }
                break;
            case CONNECTED:
                if (now.millis() > triggerTime) {
                    sessionQuickfix.trySend(testRequestEncoder);
                    triggerTime = System.currentTimeMillis() + Duration.ofSeconds(3).toMillis();
//                    sessionWriter.send(testRequestEncoder)
                }

                break;
            case IDLE:
                break;
        }


        return library.poll(FRAGMENT_LIMIT);
    }

    private void followSessionPOC(FixLibrary library) {
        SessionHeaderEncoder sessionHeaderEncoder = new HeaderEncoder();
        sessionHeaderEncoder
                .beginString("FIX.4.4")
                .targetCompID("TEST_TARGET1")
                .senderCompID("TEST_SENDER1");
        reply2 = library.followerSession(sessionHeaderEncoder, Duration.ofSeconds(5).toMillis());
    }

    @Override
    public void onClose() {
        sessionQuickfix.startLogout();
        while (sessionQuickfix.state() != SessionState.DISCONNECTED) {
            idleStrategy.idle(library.poll(FRAGMENT_LIMIT));
        }
    }

    @Override
    public String roleName() {
        return "session";
    }

    private enum State {
        CONNECT, CONNECTED, WAITING_CONNECTION, WAITING_FOLLOWER, IDLE
    }
}
