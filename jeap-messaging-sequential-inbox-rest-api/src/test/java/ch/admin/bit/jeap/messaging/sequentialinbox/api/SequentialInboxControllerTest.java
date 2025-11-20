package ch.admin.bit.jeap.messaging.sequentialinbox.api;

import ch.admin.bit.jeap.messaging.sequentialinbox.api.model.SequenceInstanceDto;
import ch.admin.bit.jeap.messaging.sequentialinbox.jpa.MessageRepository;
import ch.admin.bit.jeap.messaging.sequentialinbox.jpa.SequenceInstanceRepository;
import ch.admin.bit.jeap.messaging.sequentialinbox.persistence.SequenceInstance;
import ch.admin.bit.jeap.messaging.sequentialinbox.persistence.SequenceInstanceState;
import ch.admin.bit.jeap.messaging.sequentialinbox.persistence.SequencedMessage;
import ch.admin.bit.jeap.security.resource.semanticAuthentication.SemanticApplicationRole;
import ch.admin.bit.jeap.security.resource.token.JeapAuthenticationToken;
import ch.admin.bit.jeap.security.test.resource.JeapAuthenticationTestTokenBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.http.MediaType;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.test.web.servlet.MockMvc;

import java.time.Duration;
import java.util.List;
import java.util.Optional;

import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.authentication;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.csrf;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(controllers = SequentialInboxController.class)
@ContextConfiguration(classes = RestApiTestContext.class)
@AutoConfigureMockMvc
class SequentialInboxControllerTest {

    @Autowired
    private MockMvc mockMvc;
    @MockitoBean
    private MessageRepository messageRepository;
    @MockitoBean
    private SequenceInstanceRepository sequenceInstanceRepository;
    @MockitoBean
    private SequenceInstanceDtoFactory sequenceInstanceDtoFactory;

    private static final SemanticApplicationRole VIEW_ROLE = SemanticApplicationRole.builder()
            .system("jme")
            .resource("sequentialinbox")
            .operation("view")
            .build();

    private static final SemanticApplicationRole WRITE_ROLE = SemanticApplicationRole.builder()
            .system("jme")
            .resource("sequentialinbox")
            .operation("write")
            .build();

    private static final SemanticApplicationRole FOO_ROLE = SemanticApplicationRole.builder()
            .system("jme")
            .resource("sequentialinbox")
            .operation("foo")
            .build();


    @ParameterizedTest
    @ValueSource(strings = {
            "/api/sequences?queryType=EXPIRED",
            "/api/sequences?queryType=EXPIRING",
            "/api/sequences/sequenceName/contextId"})
    void testGetRequests_whenNoReadRole_thenReturnsForbidden(String uri) throws Exception {
        mockMvc.perform(get(uri)
                        .with(authentication(createAuthenticationForUserRoles(FOO_ROLE)))
                        .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isForbidden());
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "/api/sequences?queryType=EXPIRED",
            "/api/sequences?queryType=EXPIRING",
            "/api/sequences/sequenceName/contextId"})
    void testGetRequests_whenNoAuthentication_thenReturnsUnauthorized(String uri) throws Exception {
        mockMvc.perform(get(uri).accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isUnauthorized());
    }

    @Test
    void testGetExpiredSequences_whenFound_thenReturnSequences() throws Exception {
        SequenceInstance expiredSequence1 = SequenceInstance.builder()
                .name("expired")
                .state(SequenceInstanceState.OPEN)
                .contextId("test1")
                .retentionPeriod(Duration.ofMinutes(60))
                .build();
        when(sequenceInstanceRepository.findAllExpired(any(Pageable.class))).thenReturn(new PageImpl<>(List.of(expiredSequence1)));

        mockMvc.perform(get("/api/sequences?queryType=EXPIRED")
                        .with(authentication(createAuthenticationForUserRoles(VIEW_ROLE)))
                        .accept(MediaType.APPLICATION_JSON))
                .andDo(result -> System.out.println(result.getResponse().getContentAsString()))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.content[0].name", is("expired")));
    }

    @Test
    void testGetExpiringSequences_whenFound_thenReturnSequences() throws Exception {
        SequenceInstance expiredSequence1 = SequenceInstance.builder()
                .name("expiring")
                .state(SequenceInstanceState.OPEN)
                .contextId("test1")
                .retentionPeriod(Duration.ofMinutes(60))
                .build();
        when(sequenceInstanceRepository.findAllWithRetentionPeriodElapsed75Percent(any(Pageable.class))).thenReturn(new PageImpl<>(List.of(expiredSequence1)));

        mockMvc.perform(get("/api/sequences?queryType=EXPIRING")
                        .with(authentication(createAuthenticationForUserRoles(VIEW_ROLE)))
                        .accept(MediaType.APPLICATION_JSON))
                .andDo(result -> System.out.println(result.getResponse().getContentAsString()))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.content[0].name", is("expiring")));
    }

    @Test
    void testGetSequenceDetails_whenFound_thenReturnSequenceInstanceDto() throws Exception {
        SequenceInstance sequenceInstance = SequenceInstance.builder()
                .name("sequenceName")
                .contextId("contextId")
                .state(SequenceInstanceState.OPEN)
                .retentionPeriod(Duration.ofMinutes(60))
                .build();
        ReflectionTestUtils.setField(sequenceInstance, "id", 1L);
        when(sequenceInstanceRepository.findByNameAndContextId("sequenceName", "contextId"))
                .thenReturn(java.util.Optional.of(sequenceInstance));
        when(messageRepository.findAllBySequenceInstanceId(1L)).thenReturn(List.of());
        when(sequenceInstanceDtoFactory.fromSequenceInstance(any(SequenceInstance.class), any(), any()))
                .thenReturn(new SequenceInstanceDto(
                        sequenceInstance.getId(),
                        sequenceInstance.getName(),
                        sequenceInstance.getContextId(),
                        sequenceInstance.getState(),
                        sequenceInstance.getCreatedAt(),
                        sequenceInstance.getClosedAt(),
                        sequenceInstance.getRetainUntil(),
                        sequenceInstance.getRemoveAfter(),
                        sequenceInstance.getPendingAction(), List.of()
                ));

        mockMvc.perform(get("/api/sequences/sequenceName/contextId")
                        .with(authentication(createAuthenticationForUserRoles(VIEW_ROLE)))
                        .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.name", is("sequenceName")))
                .andExpect(jsonPath("$.contextId", is("contextId")));
    }

    @Test
    void testGetSequenceDetails_whenNotFound_thenReturnNotFound() throws Exception {
        when(sequenceInstanceRepository.findByNameAndContextId("sequenceName", "contextId"))
                .thenReturn(java.util.Optional.empty());

        mockMvc.perform(get("/api/sequences/sequenceName/contextId")
                        .with(authentication(createAuthenticationForUserRoles(VIEW_ROLE)))
                        .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isNotFound());
    }

    @Test
    void testSetSequenceInstancePendingAction_whenFound_thenReturnOk() throws Exception {
        SequenceInstance sequenceInstance = SequenceInstance.builder()
                .name("sequenceName")
                .contextId("contextId")
                .state(SequenceInstanceState.OPEN)
                .retentionPeriod(Duration.ofMinutes(60))
                .build();
        ReflectionTestUtils.setField(sequenceInstance, "id", 1L);
        when(sequenceInstanceRepository.findById(1L)).thenReturn(java.util.Optional.of(sequenceInstance));

        mockMvc.perform(put("/api/sequences/1/pending-action")
                        .param("pendingAction", "CLOSE")
                        .with(authentication(createAuthenticationForUserRoles(WRITE_ROLE)))
                .with(csrf()))
                .andExpect(status().isOk());
    }

    @Test
    void testSetSequenceInstancePendingAction_whenNotFound_thenReturnNotFound() throws Exception {
        when(sequenceInstanceRepository.findById(1L)).thenReturn(Optional.empty());

        mockMvc.perform(put("/api/sequences/1/pending-action")
                        .param("pendingAction", "CLOSE")
                        .with(authentication(createAuthenticationForUserRoles(WRITE_ROLE)))
                        .with(csrf()))
                .andExpect(status().isNotFound());
    }

    @Test
    void testSetSequencedMessagePendingAction_whenFound_thenReturnOk() throws Exception {
        SequencedMessage sequencedMessage = SequencedMessage.builder()
                        .build();
        when(messageRepository.findById(1L)).thenReturn(java.util.Optional.of(sequencedMessage));

        mockMvc.perform(put("/api/messages/1/pending-action")
                        .param("pendingAction", "EXPIRE")
                        .with(authentication(createAuthenticationForUserRoles(WRITE_ROLE)))
                        .with(csrf()))
                .andExpect(status().isOk());
    }

    @Test
    void testSetSequencedMessagePendingAction_whenNotFound_thenReturnNotFound() throws Exception {
        when(messageRepository.findById(1L)).thenReturn(Optional.empty());

        mockMvc.perform(put("/api/messages/1/pending-action")
                        .param("pendingAction", "EXPIRE")
                        .with(authentication(createAuthenticationForUserRoles(WRITE_ROLE)))
                        .with(csrf()))
                .andExpect(status().isNotFound());
    }


    private JeapAuthenticationToken createAuthenticationForUserRoles(SemanticApplicationRole... userroles) {
        return JeapAuthenticationTestTokenBuilder.create().withUserRoles(userroles).build();
    }

}
