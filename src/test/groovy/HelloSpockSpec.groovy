import com.galibots.slackRealTime.SlackAuthenticator

class HelloSpockSpec extends spock.lang.Specification {

    def "Test authentication with Slack"() {

        given:

        def env = System.getenv()
        def token = env['SLACK_TOKEN']
        def auth = new SlackAuthenticator()

        when:

        def wssUrl = auth.authenticateWithSlack(token)

        then:

        wssUrl != null
    }
}
