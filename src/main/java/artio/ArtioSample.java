package artio;

import io.aeron.archive.Archive;
import io.aeron.archive.ArchiveThreadingMode;
import io.aeron.archive.ArchivingMediaDriver;
import io.aeron.driver.MediaDriver;
import io.aeron.logbuffer.ControlledFragmentHandler;
import org.agrona.DirectBuffer;
import org.agrona.collections.IntHashSet;
import org.agrona.concurrent.AgentRunner;
import org.agrona.concurrent.SleepingIdleStrategy;
import uk.co.real_logic.artio.ArtioLogHeader;
import uk.co.real_logic.artio.CommonConfiguration;
import uk.co.real_logic.artio.engine.EngineConfiguration;
import uk.co.real_logic.artio.engine.FixEngine;
import uk.co.real_logic.artio.engine.logger.FixArchiveScanner;
import uk.co.real_logic.artio.library.*;
import uk.co.real_logic.artio.messages.DisconnectReason;
import uk.co.real_logic.artio.messages.FixMessageDecoder;
import uk.co.real_logic.artio.session.Session;

import static io.aeron.CommonContext.IPC_CHANNEL;
import static io.aeron.archive.client.AeronArchive.Configuration.CONTROL_CHANNEL_PROP_NAME;
import static io.aeron.archive.client.AeronArchive.Configuration.CONTROL_RESPONSE_CHANNEL_PROP_NAME;
import static io.aeron.driver.ThreadingMode.SHARED;
import static io.aeron.logbuffer.ControlledFragmentHandler.Action.CONTINUE;
import static java.util.Collections.singletonList;

public class ArtioSample {

    public static final String AERON_DIR_NAME = "client-aeron";
    private static final String ARCHIVE_DIR_NAME = "client-aeron-archive";
    private static final String CONTROL_REQUEST_CHANNEL = "aeron:udp?endpoint=localhost:7010";
    private static final String CONTROL_RESPONSE_CHANNEL = "aeron:udp?endpoint=localhost:7020";
    private static final String RECORDING_EVENTS_CHANNEL = "aeron:udp?control-mode=dynamic|control=localhost:7030";


    public static void main(final String[] args) {
        System.setProperty(CONTROL_CHANNEL_PROP_NAME, CONTROL_REQUEST_CHANNEL);
        System.setProperty(CONTROL_RESPONSE_CHANNEL_PROP_NAME, CONTROL_RESPONSE_CHANNEL);

        final EngineConfiguration configuration = new EngineConfiguration()
                .libraryAeronChannel(IPC_CHANNEL)
                .monitoringFile("fix-client/engineCounters")
                .logFileDir("client-logs");

        configuration.aeronArchiveContext()
                .aeronDirectoryName(AERON_DIR_NAME)
                .controlRequestChannel(CONTROL_REQUEST_CHANNEL)
                .controlResponseChannel(CONTROL_RESPONSE_CHANNEL);

        configuration.aeronContext()
                .aeronDirectoryName(AERON_DIR_NAME);

        final MediaDriver.Context context = new MediaDriver.Context()
                .threadingMode(SHARED)
                .dirDeleteOnStart(true)
                .aeronDirectoryName(AERON_DIR_NAME);

        final Archive.Context archiveContext = new Archive.Context()
                .threadingMode(ArchiveThreadingMode.SHARED)
                .deleteArchiveOnStart(true)
                .aeronDirectoryName(AERON_DIR_NAME)
                .archiveDirectoryName(ARCHIVE_DIR_NAME);

        archiveContext
                .controlChannel(CONTROL_REQUEST_CHANNEL)
                .replicationChannel("aeron:udp?endpoint=localhost:0");

        try (ArchivingMediaDriver driver = ArchivingMediaDriver.launch(context, archiveContext)) {
            try (FixEngine engine = FixEngine.launch(configuration)) {
                final SleepingIdleStrategy idleStrategy = new SleepingIdleStrategy(100);

                final LibraryConfiguration libraryConfiguration = new LibraryConfiguration()
                        .sessionAcquireHandler((session, acquiredInfo) -> onConnect())
                        .libraryAeronChannels(singletonList(IPC_CHANNEL));

                libraryConfiguration.aeronContext()
                        .aeronDirectoryName(AERON_DIR_NAME);
                try (FixLibrary library = blockingConnect(libraryConfiguration)) {
                    AgentRunner sessionAgent = new AgentRunner(
                            idleStrategy,
                            e -> {
                            },
                            null,
                            new SessionAgent(library, idleStrategy)
                    );
                    AgentRunner.startOnThread(sessionAgent);

//                    Executors.newSingleThreadExecutor().execute(ArtioSample::scanArchive);

                    while (true) {
                        Thread.yield();
                    }
                }
            }
        }
    }

    private static void scanArchive() {
        try (FixArchiveScanner fixArchiveScanner = new FixArchiveScanner(
                new FixArchiveScanner.Configuration()
                        .aeronDirectoryName(AERON_DIR_NAME)
                        .idleStrategy(CommonConfiguration.backoffIdleStrategy()))) {
            System.out.println("Scan Archive");
            IntHashSet hashSet = new IntHashSet();
            hashSet.add(CommonConfiguration.DEFAULT_OUTBOUND_LIBRARY_STREAM);
            hashSet.add(CommonConfiguration.DEFAULT_INBOUND_LIBRARY_STREAM);

            fixArchiveScanner.scan(
                    IPC_CHANNEL,
                    hashSet,
                    ArtioSample::onMessage,
                    null,
                    true,
                    EngineConfiguration.DEFAULT_ARCHIVE_SCANNER_STREAM
            );
        } catch(Exception e) {
            System.err.println(e);
        }
        System.out.println("End of Scanning");
    }

    private static void onMessage(FixMessageDecoder fixMessageDecoder, DirectBuffer directBuffer, int i, int i1, ArtioLogHeader artioLogHeader) {
        System.out.println("new message");
        System.out.println(fixMessageDecoder.body());
    }

    public static FixLibrary blockingConnect(final LibraryConfiguration configuration) {
        final FixLibrary library = FixLibrary.connect(configuration);
        while (!library.isConnected()) {
            library.poll(1);
            Thread.yield();
        }
        return library;
    }

    private static SessionHandler onConnect() {
        return new SessionHandler() {
            @Override
            public ControlledFragmentHandler.Action onMessage(DirectBuffer directBuffer, int i, int i1, int i2, Session session, int i3, long l, long l1, long l2, OnMessageInfo onMessageInfo) {
                return CONTINUE;
            }

            @Override
            public void onTimeout(int i, Session session) {

            }

            @Override
            public void onSlowStatus(int i, Session session, boolean b) {

            }

            @Override
            public ControlledFragmentHandler.Action onDisconnect(int i, Session session, DisconnectReason disconnectReason) {
                return CONTINUE;
            }

            @Override
            public void onSessionStart(Session session) {
                System.out.println(session.toString() + " connected");
            }
        };
    }


}
