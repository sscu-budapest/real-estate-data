
const serverURL = 'http://localhost:5500/';
const urlRespEndPoint = serverURL + 'url'


function postBackContent(message) {
    console.log("post-message", message)
    const data = {
        pageSource: message.content,
        pageUrl: message.url,
        registerUrls: message.registerUrls
    };

    if (message.url) {
        const options = {
            method: 'POST',
            body: JSON.stringify(data),
            headers: {
                'Content-Type': 'application/json'
            }
        };

        fetch(serverURL, options)
            .then((response) => {
                if (response.ok) {
                    console.log('Page source sent successfully.');
                    console.log(response.body);
                } else {
                    console.error('Failed to send page source.');
                }
                if (message.askNext) resend()
            })
            .catch((error) => {
                console.error('Error:', error);
            });
    } else {
        if (message.askNext) resend();
    }


}

async function tabSender(command, payload) {
    console.log("tabSender command:", command, "payload:", payload)
    await sleep(1000)
    chrome.tabs
        .query({
            currentWindow: true,
            active: true,
        })
        .then((ts) => {
            if (ts.length == 0) {
                console.log("no tabs found, retrying")
                return tabSender(command, payload);
            }
            console.log("found tabs to send to", ts)
            tab = ts[0]
            console.log("sending command", command, "payload", payload, "to tab", tab)
            chrome.tabs.sendMessage(tab.id, { command, ...(payload || {}) })
            console.log("sent")
        }).catch((e) => {
            console.log("fucked up tab")
            console.log(e)
            tabSender(command, payload)
        })
}

async function resend() {
    await sleep(Math.floor(Math.random() * 1300) + 3700)
    getNextUrlAndSentToTab()
    await sleep(Math.floor(Math.random() * 3200) + 1800)
    tabSender("go-on")
}

function getNextUrlAndSentToTab() {
    return fetch(urlRespEndPoint).then((urlResp) => {
        if (urlResp.ok) {
            console.log("sending next url to tabs", urlResp)
            urlResp.text().then((navUrl) => tabSender("next-url", { navUrl }).catch((e) => {
                console.log("fucked up in next send")
                console.log(e)
            }))
            console.log("sent url to tabs", urlResp)
            return true
        } else {
            console.log("didn't receive url", urlResp)
            return false
        }
    }).catch((error) => {
        console.log('url fetch error', error);
    });
}

function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

async function initWithSomeWaiting() {
    console.log("started waiting for urls")
    for (const i of Array(300)) {
        console.log("getting next url")
        await sleep(3000)
        if (await getNextUrlAndSentToTab()) {

            await sleep(Math.floor(Math.random() * 5200) + 1800)
            tabSender("go-on")
            return;
        }
        console.log("retrying getting next url")
    }
}

chrome.runtime.onMessage.addListener(postBackContent);
chrome.commands.onCommand.addListener(initWithSomeWaiting);
