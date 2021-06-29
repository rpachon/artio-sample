package artio;

import org.agrona.concurrent.Agent;
import org.agrona.concurrent.SleepingIdleStrategy;
import uk.co.real_logic.artio.builder.TestRequestEncoder;
import uk.co.real_logic.artio.library.FixLibrary;
import uk.co.real_logic.artio.messages.SessionState;
import uk.co.real_logic.artio.session.Session;

import java.time.Clock;
import java.time.Duration;

public class SessionAgent implements Agent {

    public static final int FRAGMENT_LIMIT = 10;
    private final FixLibrary library;
    private final SleepingIdleStrategy idleStrategy;
    private final Session session;

    private final Clock now = Clock.systemDefaultZone();
    private long triggerTime;
    private TestRequestEncoder testRequestEncoder;

    public SessionAgent(FixLibrary library, SleepingIdleStrategy idleStrategy, Session session) {
        this.library = library;
        this.idleStrategy = idleStrategy;
        this.session = session;
    }

    @Override
    public void onStart() {
        testRequestEncoder = new TestRequestEncoder();
        testRequestEncoder.testReqID("Hello World");
    }

    @Override
    public int doWork() throws Exception {
        if (now.millis() > triggerTime) {
            session.trySend(testRequestEncoder);
            triggerTime = System.currentTimeMillis() + Duration.ofSeconds(3).toMillis();
        }

        return library.poll(FRAGMENT_LIMIT);
    }

    @Override
    public void onClose() {
        session.startLogout();
        while (session.state() != SessionState.DISCONNECTED) {
            idleStrategy.idle(library.poll(FRAGMENT_LIMIT));
        }
    }

    @Override
    public String roleName() {
        return "session";
    }
}
