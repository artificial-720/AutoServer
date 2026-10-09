package me.artificial.autoserver.velocity;

import com.velocitypowered.api.proxy.Player;
import com.velocitypowered.api.proxy.ProxyServer;
import com.velocitypowered.api.proxy.server.RegisteredServer;
import com.velocitypowered.api.proxy.server.ServerInfo;
import com.velocitypowered.api.proxy.server.ServerPing;
import com.velocitypowered.api.scheduler.ScheduledTask;
import com.velocitypowered.api.scheduler.Scheduler;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

class ServerManagerTest {
    private final AutoServer plugin = mock(AutoServer.class);
    private final AutoServerLogger logger = mock(AutoServerLogger.class);
    private final Configuration configuration = mock(Configuration.class);
    private final ProxyServer proxy = mock(ProxyServer.class);
    private final Scheduler scheduler = mock(Scheduler.class);
    private final Scheduler.TaskBuilder taskBuilder = mock(Scheduler.TaskBuilder.class);
    private final ScheduledTask scheduledTask = mock(ScheduledTask.class);
    private final RegisteredServer server = mock(RegisteredServer.class);
    private final ServerInfo serverInfo = mock(ServerInfo.class);

    private ServerManager serverManager;
    private Runnable scheduledShutdown;

    @BeforeEach
    void setUp() {
        when(plugin.getLogger()).thenReturn(logger);
        when(plugin.getConfig()).thenReturn(configuration);
        when(plugin.getProxy()).thenReturn(proxy);
        when(proxy.getScheduler()).thenReturn(scheduler);
        when(server.getServerInfo()).thenReturn(serverInfo);
        when(serverInfo.getName()).thenReturn("survival");
        when(configuration.getAutoShutdownDelay(server)).thenReturn(1800L);
        when(server.getPlayersConnected()).thenReturn(Collections.emptyList());
        when(scheduler.buildTask(eq(plugin), any(Runnable.class))).thenAnswer(invocation -> {
            scheduledShutdown = invocation.getArgument(1);
            return taskBuilder;
        });
        when(taskBuilder.delay(any(Duration.class))).thenReturn(taskBuilder);
        when(taskBuilder.schedule()).thenReturn(scheduledTask);

        serverManager = new ServerManager(plugin);
    }

    @Test
    void doesNotScheduleWhenPlayersAreConnected() {
        when(server.getPlayersConnected()).thenReturn(List.of(mock(Player.class)));

        serverManager.scheduleShutdownServer(server);

        verify(scheduler, never()).buildTask(any(), any(Runnable.class));
    }

    @Test
    void schedulesOnlyOneTaskForAQuietServer() {
        serverManager.scheduleShutdownServer(server);
        serverManager.scheduleShutdownServer(server);

        verify(scheduler, times(1)).buildTask(eq(plugin), any(Runnable.class));
        verify(taskBuilder, times(1)).schedule();
    }

    @Test
    void doesNotRescheduleWhileServerIsStopping() {
        when(server.ping()).thenReturn(CompletableFuture.completedFuture(mock(ServerPing.class)));
        serverManager.getServerStatus(server).setStatus(ServerStatus.Status.STOPPING);

        serverManager.scheduleShutdownServer(server);

        verify(scheduler, never()).buildTask(any(), any(Runnable.class));
    }

    @Test
    void scheduledTaskSkipsShutdownWhenAPlayerJoinsBeforeItRuns() {
        serverManager.scheduleShutdownServer(server);
        when(server.getPlayersConnected()).thenReturn(List.of(mock(Player.class)));

        assertDoesNotThrow(scheduledShutdown::run);

        verify(server, never()).ping();
        verify(scheduler, times(1)).buildTask(eq(plugin), any(Runnable.class));
    }

    @Test
    void scheduledTaskSkipsShutdownWhenPlayersAppearDuringTheFinalCheck() {
        serverManager.scheduleShutdownServer(server);
        when(server.getPlayersConnected()).thenReturn(Collections.emptyList(), List.of(mock(Player.class)));

        assertDoesNotThrow(scheduledShutdown::run);
        verify(server, never()).ping();
    }

    @Test
    void scheduledTaskSkipsStopCommandWhenPlayerJoinsDuringPing() {
        CompletableFuture<ServerPing> pendingPing = new CompletableFuture<>();
        ServerPing response = mock(ServerPing.class);
        when(server.ping()).thenReturn(CompletableFuture.completedFuture(response), pendingPing);
        serverManager.scheduleShutdownServer(server);

        scheduledShutdown.run();
        when(server.getPlayersConnected()).thenReturn(List.of(mock(Player.class)));
        pendingPing.complete(response);

        verify(configuration, never()).getStopCommand(server);
        verify(logger).info("Skipping automatic shutdown of server {} because players are connected", "survival");
    }
}
