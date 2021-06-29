package quickfix;

import java.io.IOException;
import java.io.InputStream;

public class AcceptorQuickfix implements Application {

    public static void main(String[] args) throws ConfigError {
        SessionSettings settings = initSettings();

        Application acceptor = new AcceptorQuickfix();
        NoopStoreFactory noopStoreFactory = new NoopStoreFactory();


        SocketAcceptor socketAcceptor = new SocketAcceptor(acceptor, noopStoreFactory, settings, new DefaultMessageFactory());

        socketAcceptor.start();
    }

    private static SessionSettings initSettings() {
        try (InputStream inputStream = AcceptorQuickfix.class.getClassLoader().getResource("acceptor.cfg").openStream()) {
            return new SessionSettings(inputStream);
        } catch (IOException | ConfigError e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public void onCreate(SessionID sessionID) {

    }

    @Override
    public void onLogon(SessionID sessionID) {

    }

    @Override
    public void onLogout(SessionID sessionID) {

    }

    @Override
    public void toAdmin(Message message, SessionID sessionID) {
        System.out.println(message);
    }

    @Override
    public void fromAdmin(Message message, SessionID sessionID) throws FieldNotFound, IncorrectDataFormat, IncorrectTagValue, RejectLogon {
        System.out.println(message);
    }

    @Override
    public void toApp(Message message, SessionID sessionID) throws DoNotSend {
        System.out.println(message);
    }

    @Override
    public void fromApp(Message message, SessionID sessionID) throws FieldNotFound, IncorrectDataFormat, IncorrectTagValue, UnsupportedMessageType {
        System.out.println(message);
    }
}
