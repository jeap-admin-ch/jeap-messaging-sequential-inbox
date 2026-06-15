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
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.http.MediaType;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.web.util.UriComponentsBuilder;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

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
@Slf4j
class SequentialInboxControllerTest {

    private static final String SEQUENCE_NAME = "sequenceName";
    private static final String CONTEXT_ID = "contextId";
    private static final String PENDING_ACTION = "pendingAction";
    private static final String SEQUENTIAL_INBOX = "sequentialinbox";

    private static final String API_SEQUENCES_BASE = "/api/sequences";
    private static final String API_MESSAGES_BASE = "/api/messages";

    private static final String SEQUENCE_DETAILS_PATH = UriComponentsBuilder.fromPath(API_SEQUENCES_BASE)
            .pathSegment(SEQUENCE_NAME, CONTEXT_ID).toUriString();
    private static final String SEQUENCE_PENDING_ACTION_PATH = UriComponentsBuilder.fromPath(API_SEQUENCES_BASE)
            .pathSegment("1", "pending-action").toUriString();
    private static final String MESSAGE_PENDING_ACTION_PATH = UriComponentsBuilder.fromPath(API_MESSAGES_BASE)
            .pathSegment("1", "pending-action").toUriString();
    private static final String EXPIRED_QUERY_PATH = API_SEQUENCES_BASE + "?queryType=EXPIRED";
    private static final String EXPIRING_QUERY_PATH = API_SEQUENCES_BASE + "?queryType=EXPIRING";

    private static final SemanticApplicationRole VIEW_ROLE = SemanticApplicationRole.builder()
            .system("jme")
            .resource(SEQUENTIAL_INBOX)
            .operation("view")
            .build();

    private static final SemanticApplicationRole WRITE_ROLE = SemanticApplicationRole.builder()
            .system("jme")
            .resource(SEQUENTIAL_INBOX)
            .operation("write")
            .build();

    private static final SemanticApplicationRole FOO_ROLE = SemanticApplicationRole.builder()
            .system("jme")
            .resource(SEQUENTIAL_INBOX)
            .operation("foo")
            .build();

    private final MockMvc mockMvc;
    @MockitoBean
    private MessageRepository messageRepository;
    @MockitoBean
    private SequenceInstanceRepository sequenceInstanceRepository;
    @MockitoBean
    private SequenceInstanceDtoFactory sequenceInstanceDtoFactory;

    @Autowired
    SequentialInboxControllerTest(MockMvc mockMvc) {
        this.mockMvc = mockMvc;
    }


    @ParameterizedTest
    @MethodSource("readOnlyEndpoints")
    void getRequestsWhenNoReadRoleThenReturnsForbidden(String uri) throws Exception {
        mockMvc.perform(get(uri)
                        .with(authentication(createAuthenticationForUserRoles(FOO_ROLE)))
                        .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isForbidden());
    }

    @ParameterizedTest
    @MethodSource("readOnlyEndpoints")
    void getRequestsWhenNoAuthenticationThenReturnsUnauthorized(String uri) throws Exception {
        mockMvc.perform(get(uri).accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isUnauthorized());
    }

    private static Stream<String> readOnlyEndpoints() {
        return Stream.of(EXPIRED_QUERY_PATH, EXPIRING_QUERY_PATH, SEQUENCE_DETAILS_PATH);
    }

    @Test
    void getExpiredSequencesWhenFoundThenReturnsSequences() throws Exception {
        SequenceInstance expiredSequence1 = SequenceInstance.builder()
                .name("expired")
                .state(SequenceInstanceState.OPEN)
                .contextId("test1")
                .retentionPeriod(Duration.ofMinutes(60))
                .build();
        when(sequenceInstanceRepository.findAllExpired(any(Pageable.class))).thenReturn(new PageImpl<>(List.of(expiredSequence1)));

        mockMvc.perform(get(EXPIRED_QUERY_PATH)
                        .with(authentication(createAuthenticationForUserRoles(VIEW_ROLE)))
                        .accept(MediaType.APPLICATION_JSON))
                .andDo(result -> log.debug("{}", result.getResponse().getContentAsString()))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.content[0].name", is("expired")));
    }

    @Test
    void getExpiringSequencesWhenFoundThenReturnsSequences() throws Exception {
        SequenceInstance expiredSequence1 = SequenceInstance.builder()
                .name("expiring")
                .state(SequenceInstanceState.OPEN)
                .contextId("test1")
                .retentionPeriod(Duration.ofMinutes(60))
                .build();
        when(sequenceInstanceRepository.findAllWithRetentionPeriodElapsed75Percent(any(Pageable.class))).thenReturn(new PageImpl<>(List.of(expiredSequence1)));

        mockMvc.perform(get(EXPIRING_QUERY_PATH)
                        .with(authentication(createAuthenticationForUserRoles(VIEW_ROLE)))
                        .accept(MediaType.APPLICATION_JSON))
                .andDo(result -> log.debug("{}", result.getResponse().getContentAsString()))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.content[0].name", is("expiring")));
    }

    @Test
    void getSequenceDetailsWhenFoundThenReturnsSequenceInstanceDto() throws Exception {
        SequenceInstance sequenceInstance = SequenceInstance.builder()
                .name(SEQUENCE_NAME)
                .contextId(CONTEXT_ID)
                .state(SequenceInstanceState.OPEN)
                .retentionPeriod(Duration.ofMinutes(60))
                .build();
        ReflectionTestUtils.setField(sequenceInstance, "id", 1L);
        when(sequenceInstanceRepository.findByNameAndContextId(SEQUENCE_NAME, CONTEXT_ID))
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

        mockMvc.perform(get(SEQUENCE_DETAILS_PATH)
                        .with(authentication(createAuthenticationForUserRoles(VIEW_ROLE)))
                        .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.name", is(SEQUENCE_NAME)))
                .andExpect(jsonPath("$.contextId", is(CONTEXT_ID)));
    }

    @Test
    void getSequenceDetailsWhenNotFoundThenReturnsNotFound() throws Exception {
        when(sequenceInstanceRepository.findByNameAndContextId(SEQUENCE_NAME, CONTEXT_ID))
                .thenReturn(java.util.Optional.empty());

        mockMvc.perform(get(SEQUENCE_DETAILS_PATH)
                        .with(authentication(createAuthenticationForUserRoles(VIEW_ROLE)))
                        .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isNotFound());
    }

    @Test
    void setSequenceInstancePendingActionWhenFoundThenReturnsOk() throws Exception {
        SequenceInstance sequenceInstance = SequenceInstance.builder()
                .name(SEQUENCE_NAME)
                .contextId(CONTEXT_ID)
                .state(SequenceInstanceState.OPEN)
                .retentionPeriod(Duration.ofMinutes(60))
                .build();
        ReflectionTestUtils.setField(sequenceInstance, "id", 1L);
        when(sequenceInstanceRepository.findById(1L)).thenReturn(java.util.Optional.of(sequenceInstance));

        mockMvc.perform(put(SEQUENCE_PENDING_ACTION_PATH)
                        .param(PENDING_ACTION, "CLOSE")
                        .with(authentication(createAuthenticationForUserRoles(WRITE_ROLE)))
                .with(csrf()))
                .andExpect(status().isOk());
    }

    @Test
    void setSequenceInstancePendingActionWhenNotFoundThenReturnsNotFound() throws Exception {
        when(sequenceInstanceRepository.findById(1L)).thenReturn(Optional.empty());

        mockMvc.perform(put(SEQUENCE_PENDING_ACTION_PATH)
                        .param(PENDING_ACTION, "CLOSE")
                        .with(authentication(createAuthenticationForUserRoles(WRITE_ROLE)))
                        .with(csrf()))
                .andExpect(status().isNotFound());
    }

    @Test
    void setSequencedMessagePendingActionWhenFoundThenReturnsOk() throws Exception {
        SequencedMessage sequencedMessage = SequencedMessage.builder()
                        .build();
        when(messageRepository.findById(1L)).thenReturn(java.util.Optional.of(sequencedMessage));

        mockMvc.perform(put(MESSAGE_PENDING_ACTION_PATH)
                        .param(PENDING_ACTION, "EXPIRE")
                        .with(authentication(createAuthenticationForUserRoles(WRITE_ROLE)))
                        .with(csrf()))
                .andExpect(status().isOk());
    }

    @Test
    void setSequencedMessagePendingActionWhenNotFoundThenReturnsNotFound() throws Exception {
        when(messageRepository.findById(1L)).thenReturn(Optional.empty());

        mockMvc.perform(put(MESSAGE_PENDING_ACTION_PATH)
                        .param(PENDING_ACTION, "EXPIRE")
                        .with(authentication(createAuthenticationForUserRoles(WRITE_ROLE)))
                        .with(csrf()))
                .andExpect(status().isNotFound());
    }


    private JeapAuthenticationToken createAuthenticationForUserRoles(SemanticApplicationRole... userroles) {
        return JeapAuthenticationTestTokenBuilder.create().withUserRoles(userroles).build();
    }

}
