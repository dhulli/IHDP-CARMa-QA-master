import java.util.List;
import java.util.Optional;
import com.amazonaws.services.cloudformation.AmazonCloudFormation;
import com.amazonaws.services.cloudformation.model.AmazonCloudFormationException;
import com.amazonaws.services.cloudformation.model.DescribeStacksRequest;
import com.amazonaws.services.cloudformation.model.Stack;

abstract class RangerStackTest implements AWSCloudFormation {
    private final AmazonCloudFormation cloudFormation;
    private final String stackName;

    StackStatusWaitCondition(AmazonCloudFormation cloudFormation, String stackName) {
        this.cloudFormation = cloudFormation;
        this.stackName = stackName;
    }

    @Override
    public String getStatus() {
        return getStack().get().getStackStatus();
    }

    @Override
    public String getFailureMessage() {
        return getStack().toString();
    }

    protected Optional<Stack> getStack() {
        final List<Stack> stacks;
        try {
            stacks = cloudFormation.describeStacks(new DescribeStacksRequest().withStackName(stackName)).getStacks();
        } catch (final AmazonCloudFormationException e) {
            if (e.getStatusCode() == 400) {
                return Optional.empty();
            }
            throw e;
        }
        if (stacks.isEmpty()) {
            return Optional.empty();
        }
        if (stacks.size() > 1) {
            throw new DeploymentException("Found more than one stack for name '" + stackName + "'");
        }
        final Stack stack = stacks.get(0);
        return Optional.of(stack);
    }
}