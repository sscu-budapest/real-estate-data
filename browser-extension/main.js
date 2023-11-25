chrome.runtime.onMessage.addListener((e) => {
    console.log("main got message", e);

    const cookieButton = document.querySelector(".CybotCookiebotDialogBodyButton")
    if (cookieButton != undefined) {
        console.log("found cookie button")
        cookieButton.click();

    }

    if (e.navUrl) {
        const navUrl = e.navUrl;
        window.location.assign(navUrl);
        return
    }


    const openButton = document.querySelector(".reveal-phone-number-button-container > button")
    if (openButton != undefined) {
        console.log("found button")
        openButton.click();

    }

    const authRequer = document.querySelector("#phone-reveal-auth-required-modal");
    console.log("requer found", authRequer?.ariaHidden)
    if ((authRequer != undefined) && (authRequer.ariaHidden != 'true')) {
        console.log(authRequer, "end of the line")
        return sleepyReturn(160 * 1000, '')
    }

    if (document.URL.includes('/lista/')) {
        sleepyCrawl(Math.floor(Math.random() * 5200) + 800);

    } else {
        sleepyReturn(Math.floor(Math.random() * 2200) + 800, document.URL)
    }


})

async function sleepyReturn(delay, url) {
    await sleep(delay);
    const content = document.getElementsByTagName('html')[0].innerHTML
    chrome.runtime.sendMessage({
        content, url, registerUrls: [], askNext: true
    })

}

async function sleepyCrawl(delay) {

    // https://ingatlan.com/lista/kiado+lakas
    await sleep(delay);
    const content = document.getElementsByTagName('html')[0].innerHTML
    const nexPageLink = document.querySelector("#list > div.row.mb-5 > div.col-12.col-lg-8.col-xl-8.bg-bright.p-4.border.border-1.border-ash.rounded-5 > div:nth-child(4) > div:nth-child(3) > a");

    console.log("found element", nexPageLink)
    await sleep(delay);

    const registerUrls = [];
    const goOn = (nexPageLink != undefined);
    if (goOn) { registerUrls.push(nexPageLink.href) }

    chrome.runtime.sendMessage({
        content, url: document.URL, goOn, registerUrls, askNext: !goOn
    })

    if (goOn) { nexPageLink.click() } else {
        window.location.assign('https://myexternalip.com/raw');
    };

}

function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

async function sleepThan(ms, fun) {
    await sleep(ms)
    fun()
}