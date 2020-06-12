package io.coti.basenode.services;

import com.google.gson.Gson;
import io.coti.basenode.crypto.ClusterStampCrypto;
import io.coti.basenode.crypto.GeneralMessageCrypto;
import io.coti.basenode.crypto.GetClusterStampFileNamesCrypto;
import io.coti.basenode.data.*;
import io.coti.basenode.data.messages.GeneralVoteMessage;
import io.coti.basenode.data.messages.StateMessage;
import io.coti.basenode.data.messages.StateMessageClusterStampExecutePayload;
import io.coti.basenode.data.messages.StateMessageClusterStampHashPayload;
import io.coti.basenode.exceptions.ClusterStampException;
import io.coti.basenode.exceptions.ClusterStampValidationException;
import io.coti.basenode.exceptions.FileSystemException;
import io.coti.basenode.http.GetClusterStampFileNamesResponse;
import io.coti.basenode.http.GetNetworkVotersResponse;
import io.coti.basenode.http.Response;
import io.coti.basenode.http.SerializableResponse;
import io.coti.basenode.http.interfaces.IResponse;
import io.coti.basenode.model.Currencies;
import io.coti.basenode.model.GeneralVoteResults;
import io.coti.basenode.model.LastClusterStampVersions;
import io.coti.basenode.model.Transactions;
import io.coti.basenode.services.interfaces.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.util.SerializationUtils;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.RestTemplate;

import javax.xml.bind.DatatypeConverter;
import java.io.*;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.*;

import static io.coti.basenode.http.BaseNodeHttpStringConstants.CLUSTERSTAMP_MAJOR_NOT_FOUND;
import static io.coti.basenode.http.BaseNodeHttpStringConstants.STATUS_ERROR;

@Slf4j
@Service
public abstract class BaseNodeClusterStampService implements IClusterStampService {

    protected static final String NODE_MANAGER_VALIDATORS_ENDPOINT = "/management/validators";
    private static final int CLUSTERSTAMP_NAME_ARRAY_NOT_UPDATED_LENGTH = 2;
    private static final int CLUSTERSTAMP_UPDATE_TIME_AND_FILE_TYPE_NOT_UPDATED_INDEX = 1;
    private static final int CLUSTERSTAMP_NAME_ARRAY_LENGTH = 3;
    private static final int CLUSTERSTAMP_CONST_PREFIX_INDEX = 0;
    private static final int CLUSTERSTAMP_VERSION_TIME_INDEX = 1;
    private static final int CLUSTERSTAMP_UPDATE_TIME_AND_FILE_TYPE_INDEX = 2;
    private static final int CLUSTERSTAMP_VERSION_OR_UPDATE_TIME_AND_FILE_TYPE_ARRAY_LENGTH = 2;
    private static final int CLUSTERSTAMP_UPDATE_TIME_INDEX = 0;
    private static final int CLUSTERSTAMP_VERSION_TIME_NOT_UPDATED_INDEX = 0;
    private static final int CLUSTERSTAMP_FILE_TYPE_INDEX = 1;
    private static final int CLUSTERSTAMP_CANDIDATE_NAME_ARRAY_NOT_UPDATED_LENGTH = 3;
    private static final int CLUSTERSTAMP_CANDIDATE_NAME_ARRAY_LENGTH = 4;
    private static final int CLUSTERSTAMP_CANDIDATE_VERSION_TIME = 1;
    private static final int CLUSTERSTAMP_CANDIDATE_UPDATE_TIME = 2;
    private static final int CLUSTERSTAMP_CANDIDATE_HASH_NOT_UPDATED_INDEX = 2;
    private static final int CLUSTERSTAMP_CANDIDATE_HASH_UPDATED_INDEX = 3;
    private static final int CLUSTERSTAMP_CANDIDATE_FILE_NAME_PREFIX_INDEX = 0;
    private static final int CLUSTERSTAMP_CANDIDATE_FILE_TYPE_SUFFIX_INDEX = 1;
    private static final int CLUSTERSTAMP_CANDIDATE_PREFIX_AND_SUFFIX_ARRAY_LENGTH = 2;
    private static final int DETAILS_IN_CLUSTERSTAMP_LINE_WITHOUT_CURRENCY_HASH = 2;
    private static final int DETAILS_IN_CLUSTERSTAMP_LINE_WITH_CURRENCY_HASH = 3;
    private static final int ADDRESS_HASH_INDEX_IN_CLUSTERSTAMP_LINE = 0;
    private static final int AMOUNT_INDEX_IN_CLUSTERSTAMP_LINE = 1;
    private static final int CURRENCY_HASH_INDEX_IN_CLUSTERSTAMP_LINE = 2;
    private static final int LONG_MAX_LENGTH = 19;
    private static final String BAD_CSV_FILE_FORMAT = "Bad csv file format";
    private static final String CLUSTERSTAMP_SEGMENT_HEADER_TIMESTAMP = "# Timestamp";
    private static final String CLUSTERSTAMP_SEGMENT_HEADER_CONFIRMED_TRANSACTION_INDEX = "# Confirmed Transaction Index";
    private static final String CLUSTERSTAMP_SEGMENT_HEADER_CURRENCIES_DETAILS = "# Currencies Details";
    private static final String CLUSTERSTAMP_SEGMENT_HEADER_BALANCES_DETAILS = "# Balances Details";
    private static final String CLUSTERSTAMP_SEGMENT_HEADER_VALIDATORS_DETAILS = "# Validators Details";
    private static final String CLUSTERSTAMP_SEGMENT_HEADER_VALIDATORS_VOTES_DETAILS = "# Validators Votes Details";
    private static final String CLUSTERSTAMP_FILE_PREFIX = "clusterstamp";
    private static final String CLUSTERSTAMP_FILE_TYPE = "csv";
    private static final String CLUSTERSTAMP_ENDPOINT = "/clusterstamps";
    private static final String CLUSTERSTAMP_DELIMITER = ",";
    private static final String FOLDER_DELIMITER = "/";
    private static final String INVALID_HEADER_LINE_NOTIFICATION_AT_CLUSTERSTAMP_FILE = "Invalid header line notification at clusterstamp file %s";
    public static final int CLUSTERSTAMP_BALANCE_SEGMENT_LINE_LENGTH = 3;
    public static final int CLUSTERSTAMP_BALANCE_SEGMENT_ADDRESS_HASH_INDEX = 0;
    public static final int CLUSTERSTAMP_BALANCE_SEGMENT_CURRENCY_HASH_INDEX = 2;
    public static final int CLUSTERSTAMP_VOTES_SEGMENT_LINE_LENGTH = 6;
    public static final int CLUSTERSTAMP_VOTES_SEGMENT_SIGNER_HASH_INDEX = 1;
    public static final int CLUSTERSTAMP_VOTES_SEGMENT_SIGNATURE_R_INDEX = 2;
    public static final int CLUSTERSTAMP_VOTES_SEGMENT_SIGNATURE_S_INDEX = 3;
    public static final int CLUSTERSTAMP_VOTES_SEGMENT_VOTE_INDEX = 4;
    public static final int CLUSTERSTAMP_VOTES_SEGMENT_CLUSTER_STAMP_HASH_INDEX = 5;
    public static final int CLUSTERSTAMP_VOTES_SEGMENT_CREATE_TIME_INDEX = 0;
    protected ClusterStampNameData clusterStampName;
    @Value("${clusterstamp.folder}")
    protected String clusterStampFolder;
    protected String clusterStampBucketName;
    @Value("${aws.s3.bucket.name.candidate.clusterstamp}")
    protected String candidateClusterStampBucketName;
    @Value("${application.name}")
    private String applicationName;
    @Value("${get.cluster.stamp.from.recovery.server:true}")
    private boolean getClusterStampFromRecoveryServer;
    @Value("${node.manager.ip}")
    private String nodeManagerIp;
    @Value("${node.manager.port}")
    private String nodeManagerPort;
    private Hash candidateClusterStampHash;
    private SortedMap<Hash, CurrencyData> currencySortedMap;

    protected Instant clusterStampCreateTime;
    protected long maxIndexOfNotConfirmed;
    protected List<String> currencyClusterStampSegmentLines;
    protected List<String> balanceClusterStampSegmentLines;
    protected String voterNodesDetails;
    protected List<String> validatorsVoteClusterStampSegmentLines;

    protected String nodeManagerHttpAddress;

    @Autowired
    protected IBalanceService balanceService;
    @Autowired
    protected TrustChainConfirmationService trustChainConfirmationService;
    @Autowired
    protected Transactions transactions;
    @Autowired
    protected ClusterStampCrypto clusterStampCrypto;
    @Autowired
    private GetClusterStampFileNamesCrypto getClusterStampFileNamesCrypto;
    @Autowired
    protected INetworkService networkService;
    @Autowired
    protected IAwsService awsService;
    @Autowired
    protected LastClusterStampVersions lastClusterStampVersions;
    @Autowired
    protected BaseNodeFileSystemService fileSystemService;
    @Autowired
    protected ICurrencyService currencyService;
    @Autowired
    protected Currencies currencies;
    @Autowired
    protected ApplicationContext applicationContext;
    @Autowired
    protected IMintingService mintingService;
    @Autowired
    protected GeneralVoteResults generalVoteResults;
    @Autowired
    protected RestTemplate restTemplate;
    @Autowired
    private ClusterService clusterService;
    @Autowired
    protected GeneralMessageCrypto generalMessageCrypto;

    @Override
    public void init() {
        try {
            nodeManagerHttpAddress = "http://" + nodeManagerIp + ":" + nodeManagerPort;
            fileSystemService.createFolder(clusterStampFolder);
            initLocalClusterStampName();
            fillClusterStampNamesMap();
            if (getClusterStampFromRecoveryServer) {
                getClusterStampFromRecoveryServer();
            }
            loadAllClusterStamps();
            log.info("{} is up", this.getClass().getSimpleName());
        } catch (ClusterStampException e) {
            throw new ClusterStampException("Error at clusterstamp init.\n" + e.getMessage(), e);
        } catch (Exception e) {
            throw new ClusterStampException("Error at clusterstamp init.", e);
        }
    }

    private void initLocalClusterStampName() {
        clusterStampName = null;
    }

    protected void fillClusterStampNamesMap() {
        List<String> clusterStampFileNames = fileSystemService.listFolderFileNames(clusterStampFolder);
        for (String clusterStampFileName : clusterStampFileNames) {
            ClusterStampNameData clusterStampNameData = validateNameAndGetClusterStampNameData(clusterStampFileName);
            if (clusterStampName != null) {
                throw new ClusterStampException(String.format("Error, Multiple local clusterstamps found: [%s, %s] .Please remove excess clusterstamps and restart.", clusterStampName, getClusterStampFileName(clusterStampNameData)));
            }
            addClusterStampName(clusterStampNameData);
        }
    }

    private ClusterStampNameData validateNameAndGetClusterStampNameData(String clusterStampFileName) {
        String[] delimitedFileName = clusterStampFileName.split("_");
        if (delimitedFileName.length != CLUSTERSTAMP_NAME_ARRAY_LENGTH && delimitedFileName.length != CLUSTERSTAMP_NAME_ARRAY_NOT_UPDATED_LENGTH) {
            throw new ClusterStampValidationException(String.format("Bad cluster stamp file name: %s. Please correct clusterstamp file name and restart.", clusterStampFileName));
        }
        String clusterStampConstantPrefix = delimitedFileName[CLUSTERSTAMP_CONST_PREFIX_INDEX];
        String clusterStampUpdateTime;
        String clusterStampVersionTime;
        String clusterStampFileType;
        if (delimitedFileName.length == CLUSTERSTAMP_NAME_ARRAY_NOT_UPDATED_LENGTH) {
            String[] delimitedClusterStampVersionTimeAndFileType = validateAndGetClusterStampNameLastDelimitedPart(clusterStampFileName, delimitedFileName[CLUSTERSTAMP_UPDATE_TIME_AND_FILE_TYPE_NOT_UPDATED_INDEX]);
            clusterStampVersionTime = delimitedClusterStampVersionTimeAndFileType[CLUSTERSTAMP_VERSION_TIME_NOT_UPDATED_INDEX];
            clusterStampUpdateTime = clusterStampVersionTime;
            clusterStampFileType = delimitedClusterStampVersionTimeAndFileType[CLUSTERSTAMP_FILE_TYPE_INDEX];
        } else {
            clusterStampVersionTime = delimitedFileName[CLUSTERSTAMP_VERSION_TIME_INDEX];
            String[] delimitedClusterStampUpdateTimeAndFileType = validateAndGetClusterStampNameLastDelimitedPart(clusterStampFileName, delimitedFileName[CLUSTERSTAMP_UPDATE_TIME_AND_FILE_TYPE_INDEX]);
            clusterStampUpdateTime = delimitedClusterStampUpdateTimeAndFileType[CLUSTERSTAMP_UPDATE_TIME_INDEX];
            clusterStampFileType = delimitedClusterStampUpdateTimeAndFileType[CLUSTERSTAMP_FILE_TYPE_INDEX];
        }
        if (!validateClusterStampFileName(clusterStampConstantPrefix, clusterStampVersionTime, clusterStampUpdateTime, clusterStampFileType)) {
            throw new ClusterStampValidationException(String.format("Bad cluster stamp file name: %s. Please correct clusterstamp name and restart.", clusterStampFileName));
        }
        return new ClusterStampNameData(clusterStampVersionTime, clusterStampUpdateTime);
    }

    private String[] validateAndGetClusterStampNameLastDelimitedPart(String clusterStampFileName, String clusterStampNameLastPart) {
        String[] clusterStampNameLastDelimitedPart = clusterStampNameLastPart.split("\\.");
        if (clusterStampNameLastDelimitedPart.length != CLUSTERSTAMP_VERSION_OR_UPDATE_TIME_AND_FILE_TYPE_ARRAY_LENGTH) {
            throw new ClusterStampException(String.format("Bad cluster stamp file name: %s. Please correct clusterstamp name and restart.", clusterStampFileName));
        }
        return clusterStampNameLastDelimitedPart;
    }

    private boolean validateClusterStampFileName(String clusterStampConstantPrefix, String clusterStampVersionTime, String clusterStampUpdateTime, String clusterStampFileType) {
        return clusterStampConstantPrefix.equals(CLUSTERSTAMP_FILE_PREFIX)
                && isLong(clusterStampVersionTime)
                && isLong(clusterStampUpdateTime)
                && Long.parseLong(clusterStampUpdateTime) >= Long.parseLong(clusterStampVersionTime)
                && clusterStampFileType.equals(CLUSTERSTAMP_FILE_TYPE);
    }

    private boolean isLong(String string) {
        return NumberUtils.isDigits(string) && string.length() <= LONG_MAX_LENGTH;
    }

    protected ClusterStampNameData validateNameAndGetCandidateClusterStampNameData(String clusterStampFileName, Hash expectedClusterStampHash) {
        String[] clusterStampNamePrefixSuffixDelimitedPart = clusterStampFileName.split("\\.");
        if (clusterStampNamePrefixSuffixDelimitedPart.length != CLUSTERSTAMP_CANDIDATE_PREFIX_AND_SUFFIX_ARRAY_LENGTH) {
            throw new ClusterStampException(String.format("Bad cluster stamp file name structure: %s.", clusterStampFileName));
        }

        String[] delimitedFileName = clusterStampNamePrefixSuffixDelimitedPart[CLUSTERSTAMP_CANDIDATE_FILE_NAME_PREFIX_INDEX].split("_");
        if (delimitedFileName.length != CLUSTERSTAMP_CANDIDATE_NAME_ARRAY_NOT_UPDATED_LENGTH && delimitedFileName.length != CLUSTERSTAMP_CANDIDATE_NAME_ARRAY_LENGTH) {
            throw new ClusterStampValidationException(String.format("Bad candidate cluster stamp file name: %s. Please correct candidate clusterstamp file name", clusterStampFileName));
        }
        String clusterStampFileType = clusterStampNamePrefixSuffixDelimitedPart[CLUSTERSTAMP_CANDIDATE_FILE_TYPE_SUFFIX_INDEX];
        String clusterStampConstantPrefix = delimitedFileName[CLUSTERSTAMP_CONST_PREFIX_INDEX];

        String clusterStampVersionTime = delimitedFileName[CLUSTERSTAMP_CANDIDATE_VERSION_TIME];
        String clusterStampUpdateTime;
        String clusterStampHash;
        if (delimitedFileName.length == CLUSTERSTAMP_CANDIDATE_NAME_ARRAY_NOT_UPDATED_LENGTH) {
            clusterStampUpdateTime = clusterStampVersionTime;
            clusterStampHash = delimitedFileName[CLUSTERSTAMP_CANDIDATE_HASH_NOT_UPDATED_INDEX];
        } else {
            clusterStampUpdateTime = delimitedFileName[CLUSTERSTAMP_CANDIDATE_UPDATE_TIME];
            clusterStampHash = delimitedFileName[CLUSTERSTAMP_CANDIDATE_HASH_UPDATED_INDEX];
        }
        if (!expectedClusterStampHash.equals(new Hash(clusterStampHash))) {
            throw new ClusterStampValidationException(String.format("Bad candidate cluster stamp file name: %s. Please correct clusterstamp name and restart.", clusterStampFileName));
        }
        if (!validateCandidateClusterStampFileName(clusterStampConstantPrefix, clusterStampVersionTime, clusterStampUpdateTime, clusterStampFileType, clusterStampHash)) {
            throw new ClusterStampValidationException(String.format("Bad candidate cluster stamp file name: %s. Please correct clusterstamp name and restart.", clusterStampFileName));
        }
        return new ClusterStampNameData(clusterStampVersionTime, clusterStampUpdateTime);
    }

    private boolean validateCandidateClusterStampFileName(String clusterStampConstantPrefix, String clusterStampVersionTime, String clusterStampUpdateTime, String clusterStampFileType, String clusterStampHash) {
        try {
            DatatypeConverter.parseHexBinary(clusterStampHash);
        } catch (Exception e) {
            log.error("Illegal hash string: {}", clusterStampHash);
            return false;
        }
        return validateClusterStampFileName(clusterStampConstantPrefix, clusterStampVersionTime, clusterStampUpdateTime, clusterStampFileType);
    }

    private void loadAllClusterStamps() {
        log.info("Loading clusterstamp files");
        boolean hashCalculation = false;
        loadClusterStamp(clusterStampName, shouldUpdateClusterStampDBVersion(), hashCalculation);
    }

    protected void addClusterStampName(ClusterStampNameData clusterStampNameData) {
        clusterStampName = clusterStampNameData;
    }

    private void removeClusterStampName() {
        clusterStampName = null;
    }

    protected String getClusterStampFileName(ClusterStampNameData clusterStampNameData) {
        StringBuilder sb = getClusterStampFileNameBody(clusterStampNameData);
        return sb.append(".").append(CLUSTERSTAMP_FILE_TYPE).toString();
    }

    private StringBuilder getClusterStampFileNameBody(ClusterStampNameData clusterStampNameData) {
        Long versionTimeMillis = clusterStampNameData.getVersionTimeMillis();
        Long creationTimeMillis = clusterStampNameData.getCreationTimeMillis();
        StringBuilder sb = new StringBuilder(CLUSTERSTAMP_FILE_PREFIX);
        sb.append("_").append(versionTimeMillis.toString());
        if (!versionTimeMillis.equals(creationTimeMillis)) {
            sb.append("_").append(creationTimeMillis.toString());
        }
        return sb;
    }

    protected String getCandidateClusterStampFileName(ClusterStampNameData clusterStampNameData) {
        StringBuilder sb = getClusterStampFileNameBody(clusterStampNameData);
        sb.append("_").append(networkService.getNetworkNodeData().getNodeHash().toHexString());
        return sb.toString();
    }

    protected void loadClusterStamp(ClusterStampNameData clusterStampNameData,
                                    boolean shouldUpdateClusterStampDBVersion, boolean hashCalculation) {
        String clusterStampFileName = getClusterStampFileName(clusterStampNameData);
        log.info("Starting to load clusterstamp file {}", clusterStampFileName);
        String clusterStampFileLocation = clusterStampFolder + clusterStampFileName;
        File clusterStampFile = new File(clusterStampFileLocation);

        try (BufferedReader bufferedReader = new BufferedReader(new FileReader(clusterStampFile))) {
            boolean missingSegmentsAllowed = isMissingSegmentsAllowed();
            ClusterStampData clusterStampData = new ClusterStampData();
            String line;
            boolean prepareClusterStampLines = true;
            Map<Hash, CurrencyData> currencyMap = new HashMap<>();
            Map<Hash, ClusterStampCurrencyData> clusterStampCurrencyMap = new HashMap<>();

            line = loadClusterStampTimeStampSegment(clusterStampFileName, bufferedReader, clusterStampData);
            line = loadClusterStampTransactionIndexSegment(clusterStampFileName, bufferedReader, missingSegmentsAllowed, clusterStampData, line);
            line = loadClusterStampCurrencySegment(clusterStampFileName, bufferedReader, missingSegmentsAllowed, clusterStampData, line, prepareClusterStampLines, currencyMap);
            line = loadClusterStampBalanceSegment(clusterStampFileName, bufferedReader, clusterStampData, line, prepareClusterStampLines, currencyMap, clusterStampCurrencyMap);

            Hash clusterStampDataMessageHash = calculateClusterStampDataMessageHash(clusterStampData);
            setCandidateClusterStampHash(clusterStampDataMessageHash);

            if (!CLUSTERSTAMP_SEGMENT_HEADER_VALIDATORS_DETAILS.contentEquals(line)) {
                throw new ClusterStampValidationException(String.format(INVALID_HEADER_LINE_NOTIFICATION_AT_CLUSTERSTAMP_FILE, clusterStampFileName));
            }
            GetNetworkVotersResponse getNetworkVotersResponse = null;
            while ((line = bufferedReader.readLine()) != null && !line.contentEquals(CLUSTERSTAMP_SEGMENT_HEADER_VALIDATORS_VOTES_DETAILS)) {
                line = line.trim();
                if (line.isEmpty()) {
                    if (!missingSegmentsAllowed) {
                        throw new ClusterStampValidationException("Missing entry for cluster stamp Validators segment.");
                    } else {
                        getNetworkVotersResponse = getGetNetworkVotersResponse();
                        line = Base64.getEncoder().encodeToString(SerializationUtils.serialize(getNetworkVotersResponse));
                        log.info("Updated missing validators segment for clusterstamp file {}", clusterStampFileName);
                    }
                }
                byte[] networkVotersResponseInBytes = Base64.getDecoder().decode(line);
                getNetworkVotersResponse = (GetNetworkVotersResponse) SerializationUtils.deserialize(networkVotersResponseInBytes);
                if (getNetworkVotersResponse == null) {
                    throw new ClusterStampValidationException(String.format("Network validators Clusterstamp file %s contains invalid line", clusterStampFileName));
                }
                updateClusterStampVoterNodesDetails(line);
            }

            ArrayList<GeneralVoteMessage> generalVoteMessages = loadClusterStampVotesSegment(clusterStampFileName, bufferedReader, missingSegmentsAllowed, line, prepareClusterStampLines, clusterStampDataMessageHash);

            validateMajority(generalVoteMessages, getNetworkVotersResponse, clusterStampDataMessageHash);
            if (!hashCalculation && shouldUpdateClusterStampDBVersion) {
                currencyService.updateCurrenciesFromClusterStamp(currencyMap);
                mintingService.updateMintingAvailableMapFromClusterStamp(clusterStampCurrencyMap);
            }
        } catch (
                ClusterStampException e) {
            throw new ClusterStampException(String.format("Errors on currency clusterstamp file %s loading.%n", clusterStampFileName) + e.getMessage(), e);
        } catch (
                Exception e) {
            throw new ClusterStampException(String.format("Errors on currency clusterstamp file %s loading.", clusterStampFileName), e);
        }
    }

    private ArrayList<GeneralVoteMessage> loadClusterStampVotesSegment(String clusterStampFileName, BufferedReader bufferedReader, boolean missingSegmentsAllowed, String line, boolean prepareClusterStampLines, Hash clusterStampDataMessageHash) throws IOException {
        if (line == null || !CLUSTERSTAMP_SEGMENT_HEADER_VALIDATORS_VOTES_DETAILS.contentEquals(line)) {
            throw new ClusterStampValidationException(String.format(INVALID_HEADER_LINE_NOTIFICATION_AT_CLUSTERSTAMP_FILE, clusterStampFileName));
        }
        boolean segmentDone = false;
        ArrayList<GeneralVoteMessage> generalVoteMessages = new ArrayList<>();
        while ((line = bufferedReader.readLine()) != null && !segmentDone) {
            line = line.trim();
            if (line.isEmpty()) {
                if (!missingSegmentsAllowed) {
                    throw new ClusterStampValidationException("Missing entry for cluster stamp Balances segment.");
                } else {
                    StateMessageClusterStampHashPayload messagePayload = new StateMessageClusterStampHashPayload(clusterStampDataMessageHash);
                    GeneralVoteMessage generalVoteMessage = new GeneralVoteMessage(messagePayload, clusterStampDataMessageHash, true, clusterStampCreateTime);
                    generalMessageCrypto.signMessage(generalVoteMessage);
                    updateGeneralVoteMessageClusterStampSegment(prepareClusterStampLines, generalVoteMessage);
                    generalVoteMessages.add(generalVoteMessage);
                    segmentDone = true;
                    log.info("Updated missing votes segment for clusterstamp file {}", clusterStampFileName);
                }
            } else {
                processGeneralVoteMessageLine(line, prepareClusterStampLines, clusterStampDataMessageHash, generalVoteMessages);
            }
        }
        return generalVoteMessages;
    }

    private String loadClusterStampBalanceSegment(String clusterStampFileName, BufferedReader bufferedReader, ClusterStampData clusterStampData, String line, boolean prepareClusterStampLines, Map<Hash, CurrencyData> currencyMap, Map<Hash, ClusterStampCurrencyData> clusterStampCurrencyMap) throws IOException {
        if (!CLUSTERSTAMP_SEGMENT_HEADER_BALANCES_DETAILS.contentEquals(line)) {
            throw new ClusterStampValidationException(String.format(INVALID_HEADER_LINE_NOTIFICATION_AT_CLUSTERSTAMP_FILE, clusterStampFileName));
        }
        currencyMap.forEach((mappedCurrencyHash, mappedCurrencyData) -> {
            if (mappedCurrencyData.isConfirmed()) {
                clusterStampCurrencyMap.put(mappedCurrencyHash, new ClusterStampCurrencyData(mappedCurrencyData));
            }
        });
        while ((line = bufferedReader.readLine()) != null && !line.contentEquals(CLUSTERSTAMP_SEGMENT_HEADER_VALIDATORS_DETAILS)) {
            line = line.trim();
            if (line.isEmpty()) {
                throw new ClusterStampValidationException("Missing entry for cluster stamp Balances segment.");
            }
            String[] lineDetails = line.split(CLUSTERSTAMP_DELIMITER);
            int numOfDetailsInLine = lineDetails.length;
            if (numOfDetailsInLine != CLUSTERSTAMP_BALANCE_SEGMENT_LINE_LENGTH) {
                throw new ClusterStampValidationException(BAD_CSV_FILE_FORMAT);
            }
            Hash addressHash = new Hash(lineDetails[CLUSTERSTAMP_BALANCE_SEGMENT_ADDRESS_HASH_INDEX]);
            BigDecimal currencyAmountInAddress = new BigDecimal(lineDetails[1]);
            Hash currencyHash = new Hash(lineDetails[CLUSTERSTAMP_BALANCE_SEGMENT_CURRENCY_HASH_INDEX]);
            generateCurrencyBalanceLine(clusterStampData, currencyHash, prepareClusterStampLines, addressHash, currencyAmountInAddress);
            fillBalanceFromLine(clusterStampData, line, clusterStampCurrencyMap, clusterStampFileName);
        }
        return line;
    }

    private String loadClusterStampCurrencySegment(String clusterStampFileName, BufferedReader bufferedReader, boolean missingSegmentsAllowed, ClusterStampData clusterStampData, String line, boolean prepareClusterStampLines, Map<Hash, CurrencyData> currencyMap) throws IOException {
        if (!CLUSTERSTAMP_SEGMENT_HEADER_CURRENCIES_DETAILS.contentEquals(line)) {
            throw new ClusterStampValidationException(String.format(INVALID_HEADER_LINE_NOTIFICATION_AT_CLUSTERSTAMP_FILE, clusterStampFileName));
        }
        CurrencyData currencyData;
        while ((line = bufferedReader.readLine()) != null && !line.contentEquals(CLUSTERSTAMP_SEGMENT_HEADER_BALANCES_DETAILS)) {
            line = line.trim();
            if (line.isEmpty()) {
                if (!missingSegmentsAllowed) {
                    throw new ClusterStampValidationException("Missing entry for cluster stamp Currencies segment.");
                } else {
                    currencyData = currencyService.getNativeCurrency();
                    if (currencyData == null) {
                        currencyService.generateNativeCurrency();
                        currencyData = currencyService.getNativeCurrency();
                        log.info("Updated missing currency segment for clusterstamp file {}", clusterStampFileName);
                    }
                }
            } else {
                byte[] currencyDataInBytes = Base64.getDecoder().decode(line);
                currencyData = (CurrencyData) SerializationUtils.deserialize(currencyDataInBytes);
            }

            updateClusterStampDataBySerializedCurrencyData(clusterStampData, prepareClusterStampLines, line);

            currencyMap.put(currencyData.getHash(), currencyData);
        }
        return line;
    }

    private String loadClusterStampTransactionIndexSegment(String clusterStampFileName, BufferedReader bufferedReader, boolean missingSegmentsAllowed, ClusterStampData clusterStampData, String line) throws IOException {
        if (!CLUSTERSTAMP_SEGMENT_HEADER_CONFIRMED_TRANSACTION_INDEX.contentEquals(line)) {
            throw new ClusterStampValidationException(String.format(INVALID_HEADER_LINE_NOTIFICATION_AT_CLUSTERSTAMP_FILE, clusterStampFileName));
        }
        long maxIndexOfNotConfirmedTransaction;
        while ((line = bufferedReader.readLine()) != null && !line.contentEquals(CLUSTERSTAMP_SEGMENT_HEADER_CURRENCIES_DETAILS)) {
            line = line.trim();
            if (line.isEmpty()) {
                if (!missingSegmentsAllowed) {
                    throw new ClusterStampValidationException("Missing entry for cluster stamp Confirmed Transaction Index segment.");
                } else {
                    maxIndexOfNotConfirmedTransaction = clusterService.getMaxIndexOfNotConfirmed();
                    log.info("Updated missing transaction index segment for clusterstamp file {}", clusterStampFileName);
                }
            } else {
                maxIndexOfNotConfirmedTransaction = Long.parseLong(line);
            }
            updateClusterStampMaxIndex(maxIndexOfNotConfirmedTransaction, clusterStampData);
        }
        return line;
    }

    private String loadClusterStampTimeStampSegment(String clusterStampFileName, BufferedReader bufferedReader, ClusterStampData clusterStampData) throws IOException {
        String line;
        Instant createTime;
        line = bufferedReader.readLine();
        if (!line.contentEquals(CLUSTERSTAMP_SEGMENT_HEADER_TIMESTAMP)) {
            throw new ClusterStampValidationException(String.format("Invalid header line notification at  clusterstamp file %s", clusterStampFileName));
        }
        while ((line = bufferedReader.readLine()) != null && !line.contentEquals(CLUSTERSTAMP_SEGMENT_HEADER_CONFIRMED_TRANSACTION_INDEX)) {
            line = line.trim();
            if (line.isEmpty()) {
                throw new ClusterStampValidationException("Missing entry for cluster stamp Timestamp segment.");
            }
            createTime = Instant.ofEpochMilli(Long.parseLong(line));
            updateClusterStampCreateTime(createTime, clusterStampData);
        }
        return line;
    }

    private void validateMajority(ArrayList<GeneralVoteMessage> generalVoteMessages, GetNetworkVotersResponse getNetworkVotersResponse, Hash clusterStampDataMessageHash) {
        if (getNetworkVotersResponse == null) {
            throw new ClusterStampValidationException("Failed to calculate votes for cluster stamp votes segment.");
        }
        List<Hash> allCurrentValidators = getNetworkVotersResponse.getAllCurrentValidators();
        long positiveVotesAmount = generalVoteMessages.stream().filter(generalVoteMessage ->
                        generalVoteMessage.isVote() &&
                                allCurrentValidators.contains(generalVoteMessage.getSignerHash()) &&
                                ((StateMessageClusterStampHashPayload) generalVoteMessage.getMessagePayload()).getClusterStampHash().equals(clusterStampDataMessageHash)
                //TODO 6/9/2020 tomer: Check about vote hash?
        ).count();
        if (positiveVotesAmount < getExpectedMajority(allCurrentValidators.size())) {
            throw new ClusterStampValidationException("Failed to reach majority for cluster stamp votes segment.");
        }
    }

    private int getExpectedMajority(int votesAmount) {
        return votesAmount / 2;
    }

    protected GetNetworkVotersResponse getGetNetworkVotersResponse() {
        return restTemplate.getForEntity(nodeManagerHttpAddress + NODE_MANAGER_VALIDATORS_ENDPOINT, GetNetworkVotersResponse.class).getBody();
    }

    private void processGeneralVoteMessageLine(String line, boolean prepareClusterStampLines, Hash clusterStampDataMessageHash, List<GeneralVoteMessage> generalVoteMessages) {
        String[] lineDetails = line.split(CLUSTERSTAMP_DELIMITER);
        int numOfDetailsInLine = lineDetails.length;
        if (numOfDetailsInLine != CLUSTERSTAMP_VOTES_SEGMENT_LINE_LENGTH) {
            throw new ClusterStampValidationException(BAD_CSV_FILE_FORMAT);
        }
        Instant generalVoteMessageCreateTime = Instant.ofEpochMilli(Long.parseLong(lineDetails[CLUSTERSTAMP_VOTES_SEGMENT_CREATE_TIME_INDEX]));
        Hash signerHash = new Hash(lineDetails[CLUSTERSTAMP_VOTES_SEGMENT_SIGNER_HASH_INDEX]);
        String voteSignatureR = lineDetails[CLUSTERSTAMP_VOTES_SEGMENT_SIGNATURE_R_INDEX];
        String voteSignatureS = lineDetails[CLUSTERSTAMP_VOTES_SEGMENT_SIGNATURE_S_INDEX];
        boolean vote = Boolean.parseBoolean(lineDetails[CLUSTERSTAMP_VOTES_SEGMENT_VOTE_INDEX]);
        Hash clusterStampHash = new Hash(lineDetails[CLUSTERSTAMP_VOTES_SEGMENT_CLUSTER_STAMP_HASH_INDEX]);

        StateMessageClusterStampHashPayload messagePayload = new StateMessageClusterStampHashPayload(clusterStampHash);
        GeneralVoteMessage generalVoteMessage = new GeneralVoteMessage(messagePayload, clusterStampHash, vote, generalVoteMessageCreateTime);
        generalVoteMessage.setSignature(new SignatureData(voteSignatureR, voteSignatureS));
        generalVoteMessage.setSignerHash(signerHash);

        if (!clusterStampHash.equals(clusterStampDataMessageHash)) {
            throw new ClusterStampValidationException("Cluster hash values don't match %s " + clusterStampHash + " " + clusterStampDataMessageHash);
        }
        updateGeneralVoteMessageClusterStampSegment(prepareClusterStampLines, generalVoteMessage);

        if (!generalMessageCrypto.verifySignature(generalVoteMessage)) {
            throw new ClusterStampValidationException(String.format("Cluster stamp general vote of %s message failed validation", signerHash));
        }
        generalVoteMessages.add(generalVoteMessage);
    }

    private boolean isMissingSegmentsAllowed() {
        return false;
    }

    public void updateGeneralVoteMessageClusterStampSegment(boolean prepareClusterStampLines, GeneralVoteMessage generalVoteMessage) {
        if (!prepareClusterStampLines) {
            return;
        }
        Instant generalVoteMessageCreateTime = generalVoteMessage.getCreateTime();
        Hash signerHash = generalVoteMessage.getSignerHash();
        String voteSignatureR = generalVoteMessage.getSignature().getR();
        String voteSignatureS = generalVoteMessage.getSignature().getS();
        boolean vote = generalVoteMessage.isVote();
        Hash clusterStampHash = ((StateMessageClusterStampHashPayload) generalVoteMessage.getMessagePayload()).getClusterStampHash();

        StringBuilder sb = new StringBuilder();
        String line = sb.append(generalVoteMessageCreateTime.toEpochMilli()).append(CLUSTERSTAMP_DELIMITER).append(signerHash.toHexString()).append(CLUSTERSTAMP_DELIMITER).
                append(voteSignatureR).append(CLUSTERSTAMP_DELIMITER).append(voteSignatureS).append(CLUSTERSTAMP_DELIMITER).
                append(vote).append(CLUSTERSTAMP_DELIMITER).append(clusterStampHash.toHexString()).append(CLUSTERSTAMP_DELIMITER).toString();
        validatorsVoteClusterStampSegmentLines.add(line);
    }

    protected void handleMissingRecoveryServer() {
        throw new ClusterStampException("Recovery server undefined.");
    }

    @Override
    public void getClusterStampFromRecoveryServer() {
        String recoveryServerAddress = networkService.getRecoveryServerAddress();
        if (recoveryServerAddress == null) {
            handleMissingRecoveryServer();
            return;
        }
        try {
            GetClusterStampFileNamesResponse getClusterStampFileNamesResponse = restTemplate.getForObject(recoveryServerAddress + CLUSTERSTAMP_ENDPOINT, GetClusterStampFileNamesResponse.class);
            if (getClusterStampFileNamesResponse == null) {
                throw new ClusterStampException(String.format("Cluster stamp retrieval failed. Null response from recovery server %s.", recoveryServerAddress));
            }
            if (!getClusterStampFileNamesCrypto.verifySignature(getClusterStampFileNamesResponse)) {
                throw new ClusterStampException(String.format("Cluster stamp retrieval failed. Bad signature for response from recovery server %s.", recoveryServerAddress));
            }
            clusterStampBucketName = getClusterStampFileNamesResponse.getClusterStampBucketName();
            handleRequiredClusterStampFiles(getClusterStampFileNamesResponse);
        } catch (HttpClientErrorException | HttpServerErrorException e) {
            throw new ClusterStampException(String.format("Clusterstamp recovery failed. Recovery server response: %s.", new Gson().fromJson(e.getResponseBodyAsString(), Response.class).getMessage()), e);
        } catch (ClusterStampException e) {
            throw new ClusterStampException("Clusterstamp recovery failed.\n" + e.getMessage(), e);
        } catch (Exception e) {
            throw new ClusterStampException("Clusterstamp recovery failed.", e);
        }
    }

    private void handleRequiredClusterStampFiles(GetClusterStampFileNamesResponse getClusterStampFileNamesResponse) {
        if (!validateResponseVersionValidity(getClusterStampFileNamesResponse)) {
            throw new ClusterStampValidationException("Recovery clusterstamp version is not valid");
        }
        ClusterStampNameData recoveryClusterStampName = getClusterStampFileNamesResponse.getClusterStampName();
        if (clusterStampName == null || recoveryClusterStampName.getVersionTimeMillis() > clusterStampName.getVersionTimeMillis()) {
            handleMissingClusterStamp(clusterStampName, recoveryClusterStampName);
        }
    }

    private boolean validateResponseVersionValidity(GetClusterStampFileNamesResponse getClusterStampFileNamesResponse) {
        LastClusterStampVersionData lastVersionData = lastClusterStampVersions.get();
        ClusterStampNameData recoveryClusterStampName = getClusterStampFileNamesResponse.getClusterStampName();
        return recoveryClusterStampName != null &&
                (lastVersionData == null || lastVersionData.getVersionTimeMillis() == null ||
                        validateVersion(recoveryClusterStampName.getVersionTimeMillis(), lastVersionData.getVersionTimeMillis()));
    }

    private boolean validateVersion(Long clusterStampFileVersion, Long clusterStampDBVersion) {
        return clusterStampFileVersion >= clusterStampDBVersion;
    }

    private void handleMissingClusterStamp(ClusterStampNameData localClusterStampName, ClusterStampNameData recoveryClusterStampName) {
        if (localClusterStampName != null) {
            removeClusterStampNameAndFile(localClusterStampName);
        }
        downloadAndAddSingleClusterStamp(recoveryClusterStampName);
    }

    @Override
    public boolean shouldUpdateClusterStampDBVersion() {
        LastClusterStampVersionData lastVersionData = lastClusterStampVersions.get();
        return lastVersionData == null || lastVersionData.getVersionTimeMillis() == null || clusterStampName.getVersionTimeMillis() > lastVersionData.getVersionTimeMillis();
    }

    @Override
    public boolean isClusterStampDBVersionExist() {
        return lastClusterStampVersions.get() != null;
    }

    @Override
    public void setClusterStampDBVersion() {
        lastClusterStampVersions.put(new LastClusterStampVersionData(clusterStampName.getVersionTimeMillis()));
        log.info("Clusterstamp version time is set to {}", Instant.ofEpochMilli(clusterStampName.getVersionTimeMillis()));
    }

    private void removeClusterStampNameAndFile(ClusterStampNameData clusterStampNameData) {
        removeClusterStampName();
        String clusterStampFileName = getClusterStampFileName(clusterStampNameData);
        String clusterStampFilePath = clusterStampFolder + clusterStampFileName;
        try {
            fileSystemService.deleteFile(clusterStampFilePath);
        } catch (Exception e) {
            throw new ClusterStampException(String.format("Failed to delete clusterstamp file %s. Please delete manually and restart.", clusterStampFileName), e);
        }
    }

    private void downloadAndAddSingleClusterStamp(ClusterStampNameData clusterStampNameData) {
        String clusterStampFileName = getClusterStampFileName(clusterStampNameData);
        String filePath = clusterStampFolder + clusterStampFileName;
        try {
            awsService.downloadFile(filePath, clusterStampBucketName);
            addClusterStampName(clusterStampNameData);
        } catch (Exception e) {
            throw new ClusterStampException(String.format("Couldn't download clusterstamp file %s.", clusterStampFileName), e);
        }
    }

    @Override
    public ResponseEntity<IResponse> getRequiredClusterStampNames() {
        GetClusterStampFileNamesResponse getClusterStampFileNamesResponse = new GetClusterStampFileNamesResponse();
        if (clusterStampName == null) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(new SerializableResponse(CLUSTERSTAMP_MAJOR_NOT_FOUND, STATUS_ERROR));
        }
        getClusterStampFileNamesResponse.setClusterStampName(clusterStampName);
        getClusterStampFileNamesResponse.setClusterStampBucketName(clusterStampBucketName);
        getClusterStampFileNamesCrypto.signMessage(getClusterStampFileNamesResponse);
        return ResponseEntity.ok(getClusterStampFileNamesResponse);
    }

    private void fillBalanceFromLine(ClusterStampData clusterStampData, String line, Map<Hash, ClusterStampCurrencyData> clusterStampCurrencyMap, String clusterStampFileName) {
        try {
            String[] lineDetails = line.split(",");
            int numOfDetailsInLine = lineDetails.length;
            if (numOfDetailsInLine != DETAILS_IN_CLUSTERSTAMP_LINE_WITH_CURRENCY_HASH && numOfDetailsInLine != DETAILS_IN_CLUSTERSTAMP_LINE_WITHOUT_CURRENCY_HASH) {
                throw new ClusterStampValidationException(BAD_CSV_FILE_FORMAT);
            }
            Hash addressHash = new Hash(lineDetails[ADDRESS_HASH_INDEX_IN_CLUSTERSTAMP_LINE]);
            BigDecimal currencyAmountInAddress = new BigDecimal(lineDetails[AMOUNT_INDEX_IN_CLUSTERSTAMP_LINE]);
            Hash currencyHash = numOfDetailsInLine == DETAILS_IN_CLUSTERSTAMP_LINE_WITH_CURRENCY_HASH && !lineDetails[CURRENCY_HASH_INDEX_IN_CLUSTERSTAMP_LINE].isEmpty() ? new Hash(lineDetails[CURRENCY_HASH_INDEX_IN_CLUSTERSTAMP_LINE]) : null;
            if (currencyHash == null) {
                currencyHash = currencyService.getNativeCurrencyHash();
            }

            validateClusterStampLineDetails(currencyAmountInAddress, currencyHash, clusterStampCurrencyMap, clusterStampFileName);
            balanceService.updateBalanceAndPreBalanceFromClusterStamp(addressHash, currencyHash, currencyAmountInAddress);
            log.trace("The address hash {} for currency hash {} was loaded from the clusterstamp {} with amount {}", addressHash, currencyHash, clusterStampFileName, currencyAmountInAddress);

            byte[] addressHashInBytes = addressHash.getBytes();
            byte[] addressCurrencyAmountInBytes = currencyAmountInAddress.stripTrailingZeros().toPlainString().getBytes();
            byte[] currencyHashInBytes = numOfDetailsInLine == DETAILS_IN_CLUSTERSTAMP_LINE_WITH_CURRENCY_HASH ? currencyHash.getBytes() : new byte[0];
            updateClusterStampDataMessage(clusterStampData, addressHashInBytes, addressCurrencyAmountInBytes, currencyHashInBytes);
        } catch (ClusterStampException e) {
            throw new ClusterStampException(String.format("Error at filling balance from line of clusterstamp %s.%n", clusterStampFileName) + e.getMessage(), e);
        } catch (Exception e) {
            throw new ClusterStampException(String.format("Error at filling balance from line of clusterstamp %s.", clusterStampFileName), e);
        }
    }

    private void validateClusterStampLineDetails(BigDecimal currencyAmountInAddress, Hash
            currencyHash, Map<Hash, ClusterStampCurrencyData> clusterStampCurrencyMap, String clusterStampFileName) {
        ClusterStampCurrencyData clusterStampCurrencyData = clusterStampCurrencyMap.get(currencyHash);
        if (clusterStampCurrencyData == null) {
            throw new ClusterStampValidationException(String.format("Currency %s in clusterstamp file %s not found at DB", currencyHash, clusterStampFileName));
        }

        int scale = clusterStampCurrencyData.getScale();
        if (currencyAmountInAddress.scale() > scale) {
            throw new ClusterStampValidationException(String.format("Scale of currency %s in clusterstamp file is wrong for amount %s.", currencyHash, currencyAmountInAddress));
        }

        BigDecimal subtractedCurrencyAmount = clusterStampCurrencyData.getAmount().subtract(currencyAmountInAddress);
        if (subtractedCurrencyAmount.compareTo(BigDecimal.ZERO) < 0) {
            throw new ClusterStampValidationException(String.format("Total amount of currency %s in clusterstamp file exceeds currency supply.", currencyHash));
        }
        clusterStampCurrencyData.setAmount(subtractedCurrencyAmount);
    }

    @Override
    public void clusterStampExecute(StateMessage stateMessage, StateMessageClusterStampExecutePayload stateMessageClusterStampExecutePayload) {
        // implemented in subclasses
    }

    private void updateClusterStampMaxIndex(long maxIndexOfNotConfirmedTransaction, ClusterStampData clusterStampData) {
        maxIndexOfNotConfirmed = maxIndexOfNotConfirmedTransaction;
        clusterStampData.incrementMessageByteSize(Long.BYTES);
        clusterStampData.getSignatureMessage().add(ByteBuffer.allocate(Long.BYTES).putLong(maxIndexOfNotConfirmedTransaction).array());
    }

    private void updateClusterStampCreateTime(Instant createTime, ClusterStampData clusterStampData) {
        clusterStampData.setCreateTime(createTime);
        clusterStampData.incrementMessageByteSize(Long.BYTES);
        clusterStampData.getSignatureMessage().add(ByteBuffer.allocate(Long.BYTES).putLong(createTime.toEpochMilli()).array());
        clusterStampCreateTime = createTime;
    }

    protected void updateClusterStampVoterNodesDetails(String voterNodesDetails) {
        this.voterNodesDetails = voterNodesDetails;
    }

    private void prepareForClusterStampCurrencySegment(ClusterStampData clusterStampData, boolean prepareClusterStampLines, CurrencyData nativeCurrency, boolean onlyNativeCurrency) {
        if (nativeCurrency != null) {
            updateClusterStampDataByCurrencyData(clusterStampData, nativeCurrency, prepareClusterStampLines);
        } else {
            throw new ClusterStampException("Unable to calculate cluster stamp. Genesis address not found.");
        }
        if (!onlyNativeCurrency) {
            sortCurrencies();
            generateCurrencyLines(clusterStampData, prepareClusterStampLines);
        }
    }

    private void updateClusterStampDataByCurrencyData(ClusterStampData clusterStampData, CurrencyData currencyData, boolean prepareClusterStampLines) {
        String serializedCurrencyData = Base64.getEncoder().encodeToString(SerializationUtils.serialize(currencyData));
        updateClusterStampDataBySerializedCurrencyData(clusterStampData, prepareClusterStampLines, serializedCurrencyData);
    }

    private void updateClusterStampDataBySerializedCurrencyData(ClusterStampData clusterStampData, boolean prepareClusterStampLines, String serializedCurrencyData) {
        byte[] currencyDataInBytes = Base64.getDecoder().decode(serializedCurrencyData);
        clusterStampData.getSignatureMessage().add(currencyDataInBytes);
        clusterStampData.incrementMessageByteSize(currencyDataInBytes.length);
        if (prepareClusterStampLines) {
            currencyClusterStampSegmentLines.add(serializedCurrencyData);
        }
    }

    private void generateCurrencyLines(ClusterStampData clusterStampData, boolean prepareClusterStampLines) {
        for (Map.Entry<Hash, CurrencyData> additionalCurrencyData : currencySortedMap.entrySet()) {
            if (additionalCurrencyData.getValue().isConfirmed() && !additionalCurrencyData.getValue().getCurrencyTypeData().getCurrencyType().equals(CurrencyType.NATIVE_COIN)) {
                updateClusterStampDataByCurrencyData(clusterStampData, additionalCurrencyData.getValue(), prepareClusterStampLines);
            }
        }
    }

    private void sortCurrencies() {
        currencySortedMap = new TreeMap();
        if (!currencies.isEmpty()) {
            currencies.forEach(currencyData -> currencySortedMap.put(currencyData.getHash(), currencyData));
        } else {
            throw new ClusterStampException("Unable to start cluster stamp. Currencies not found.");
        }
    }

    private Hash calculateClusterStampDataMessageHash(ClusterStampData clusterStampData) {
        byte[] streamArray = null;
        try {
            streamArray = IOUtils.toByteArray((InputStream) clusterStampData.getSignatureMessage());
        } catch (IOException e) {
            throw new ClusterStampException("Unable to calculate cluster stamp data message hash.");
        }
        return streamArray != null ? new Hash(streamArray) : null;
    }

    protected void prepareOnlyForNativeGenesisAddressBalanceClusterStampSegment(ClusterStampData clusterStampData, boolean prepareClusterStampLines, CurrencyData nativeCurrency) {
        throw new ClusterStampException("Attempting to create initial native currency cluster stamp");
    }

    private void prepareForBalanceClusterStampSegment(ClusterStampData clusterStampData, boolean prepareClusterStampLines, Hash nativeCurrencyHash) {
        TreeMap<Hash, BigDecimal> sortedBalance = balanceService.getSortedBalance(nativeCurrencyHash);
        generateCurrencyBalanceLines(clusterStampData, nativeCurrencyHash, sortedBalance, prepareClusterStampLines);
        currencySortedMap.forEach((currencyHash, currencyData) ->
                currencySortedMap.keySet().stream().map(hash -> currencySortedMap.get(hash))
                        .filter(additionalCurrencyData -> additionalCurrencyData.isConfirmed() && !additionalCurrencyData.getCurrencyTypeData().getCurrencyType().equals(CurrencyType.NATIVE_COIN))
                        .forEach(additionalCurrencyData -> generateCurrencyBalanceLines(clusterStampData, additionalCurrencyData.getHash(), sortedBalance, prepareClusterStampLines))
        );
    }

    private void generateCurrencyBalanceLines(ClusterStampData clusterStampData, Hash currencyHash, TreeMap<Hash, BigDecimal> sortedBalance, boolean prepareClusterStampLines) {
        for (Map.Entry<Hash, BigDecimal> entry : sortedBalance.entrySet()) {
            Hash addressHash = entry.getKey();
            BigDecimal currencyAmountInAddress = entry.getValue();
            generateCurrencyBalanceLine(clusterStampData, currencyHash, prepareClusterStampLines, addressHash, currencyAmountInAddress);
        }
    }

    private void generateCurrencyBalanceLine(ClusterStampData clusterStampData, Hash currencyHash, boolean prepareClusterStampLines, Hash addressHash, BigDecimal currencyAmountInAddress) {
        StringBuilder sb = new StringBuilder();
        String line = sb.append(addressHash).append(CLUSTERSTAMP_DELIMITER).append(currencyAmountInAddress.toString()).append(CLUSTERSTAMP_DELIMITER).append(currencyHash).toString();
        if (prepareClusterStampLines) {
            balanceClusterStampSegmentLines.add(line);
        }
        byte[] addressHashInBytes = addressHash.getBytes();
        byte[] addressCurrencyAmountInBytes = currencyAmountInAddress.stripTrailingZeros().toPlainString().getBytes();
        byte[] currencyHashInBytes = currencyHash.getBytes();
        updateClusterStampDataMessage(clusterStampData, addressHashInBytes, addressCurrencyAmountInBytes, currencyHashInBytes);
    }

    private void updateClusterStampDataMessage(ClusterStampData clusterStampData, byte[] addressHashInBytes, byte[] addressCurrencyAmountInBytes, byte[] currencyHashInBytes) {
        byte[] balanceInBytes = ByteBuffer.allocate(addressHashInBytes.length + addressCurrencyAmountInBytes.length + currencyHashInBytes.length)
                .put(addressHashInBytes).put(addressCurrencyAmountInBytes).put(currencyHashInBytes).array();
        clusterStampData.getSignatureMessage().add(balanceInBytes);
        clusterStampData.incrementMessageByteSize(balanceInBytes.length);
    }

    @Override
    public Hash getCandidateClusterStampHash() {
        return candidateClusterStampHash;
    }

    private void setCandidateClusterStampHash(Hash candidateClusterStampHash) {
        this.candidateClusterStampHash = candidateClusterStampHash;
    }

    public void prepareCandidateClusterStampHash() {
        prepareCandidateClusterStampHash(Instant.now(), false, new ClusterStampData(), false);
    }

    protected void prepareCandidateClusterStampHash(Instant createTime, boolean prepareClusterStampLines, ClusterStampData clusterStampData, boolean onlyNativeCurrency) {
        CurrencyData nativeCurrency = currencyService.getNativeCurrency();
        if (nativeCurrency == null) {
            currencyService.generateNativeCurrency();
            nativeCurrency = currencyService.getNativeCurrency();
        }

        updateClusterStampCreateTime(createTime, clusterStampData);
        long maxIndexOfNotConfirmedTransaction = clusterService.getMaxIndexOfNotConfirmed();
        updateClusterStampMaxIndex(maxIndexOfNotConfirmedTransaction, clusterStampData);
        currencyClusterStampSegmentLines = new ArrayList<>();
        prepareForClusterStampCurrencySegment(clusterStampData, prepareClusterStampLines, nativeCurrency, onlyNativeCurrency);
        balanceClusterStampSegmentLines = new ArrayList<>();
        if (onlyNativeCurrency) {
            prepareOnlyForNativeGenesisAddressBalanceClusterStampSegment(clusterStampData, prepareClusterStampLines, nativeCurrency);
        } else {
            prepareForBalanceClusterStampSegment(clusterStampData, prepareClusterStampLines, nativeCurrency.getHash());
        }

        Hash clusterStampDataMessageHash = calculateClusterStampDataMessageHash(clusterStampData);
        setCandidateClusterStampHash(clusterStampDataMessageHash);
    }

    protected GeneralVoteMessage createGeneralVoteMessage(Instant createTime, Hash clusterStampDataMessageHash) {
        StateMessageClusterStampHashPayload messagePayload = new StateMessageClusterStampHashPayload(clusterStampDataMessageHash);
        GeneralVoteMessage generalVoteMessage = new GeneralVoteMessage(messagePayload, clusterStampDataMessageHash, true, createTime);
        generalMessageCrypto.signMessage(generalVoteMessage);
        return generalVoteMessage;
    }

    public void writeClusterStamp(Instant createTime) {
        String versionTimeMillisString = String.valueOf(createTime.toEpochMilli());
        ClusterStampNameData newClusterStampNameData = new ClusterStampNameData(versionTimeMillisString, versionTimeMillisString);
        String candidateClusterStampFileName = getCandidateClusterStampFileName(newClusterStampNameData);
        String clusterStampFilename = clusterStampFolder + FOLDER_DELIMITER + candidateClusterStampFileName;

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(clusterStampFilename))) {
            writeClusterStampLine(writer, CLUSTERSTAMP_SEGMENT_HEADER_TIMESTAMP, versionTimeMillisString);
            writeClusterStampLine(writer, CLUSTERSTAMP_SEGMENT_HEADER_CONFIRMED_TRANSACTION_INDEX, Long.toString(maxIndexOfNotConfirmed));
            writeClusterStampLines(writer, CLUSTERSTAMP_SEGMENT_HEADER_CURRENCIES_DETAILS, currencyClusterStampSegmentLines);
            writeClusterStampLines(writer, CLUSTERSTAMP_SEGMENT_HEADER_BALANCES_DETAILS, balanceClusterStampSegmentLines);
            writeClusterStampLine(writer, CLUSTERSTAMP_SEGMENT_HEADER_VALIDATORS_DETAILS, voterNodesDetails);
            writeClusterStampLines(writer, CLUSTERSTAMP_SEGMENT_HEADER_VALIDATORS_VOTES_DETAILS, validatorsVoteClusterStampSegmentLines);
        } catch (IOException e) {
            throw new FileSystemException(String.format("Create and write file error. %s: %s", e.getClass().getName(), e.getMessage()));
        }
        uploadCandidateClusterStamp(clusterStampFilename);
    }

    private void writeClusterStampLine(BufferedWriter writer, String header, String line) throws IOException {
        writer.write(header);
        writer.newLine();
        writer.write(line);
        writer.newLine();
    }

    private void writeClusterStampLines(BufferedWriter writer, String header, List<String> lines) throws IOException {
        writer.write(header);
        writer.newLine();
        for (String currencyDetails : lines) {
            writer.write(currencyDetails);
            writer.newLine();
        }
    }

    private void uploadCandidateClusterStamp(String candidateClusterStampFileName) {
        awsService.uploadFileToS3(candidateClusterStampBucketName, clusterStampFolder + candidateClusterStampFileName);
    }

}