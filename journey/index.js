const primaryColor = "#2285e9";
/** 背景粒子动画 */
particlesJS("particles-js", {
  particles: {
    number: {
      value: 150,
      density: {
        enable: true,
        value_area: 800,
      },
    },
    color: {
      value: primaryColor,
    },
    shape: {
      type: "circle",
      stroke: {
        width: 0,
        color: "#000000",
      },
      polygon: {
        nb_sides: 5,
      },
    },
    opacity: {
      value: 0.2,
      random: false,
      anim: {
        enable: false,
        speed: 1,
        opacity_min: 0.1,
        sync: false,
      },
    },
    size: {
      value: 3,
      random: true,
      anim: {
        enable: false,
        speed: 40,
        size_min: 0.1,
        sync: false,
      },
    },
    line_linked: {
      enable: true,
      distance: 150,
      color: primaryColor,
      opacity: 0.2,
      width: 1,
    },
  },
  interactivity: {
    detect_on: "canvas",
    events: {
      onhover: {
        enable: false,
        mode: "grab",
      },
      resize: true,
    },
  },
  retina_detect: true,
});

document.querySelectorAll("section").forEach((item, index) => {
  item.id = `s-${index}`;
});

const calcPointY = (targetDOM) => {
  const { y, height } = targetDOM.getBoundingClientRect();
  return y + window.scrollY + height;
};

const getPaths = (startPoint, endPoint, { width, gapHeight }) => {
  const height = endPoint.y - startPoint.y;
  const gapNumber = Math.round(height / gapHeight);

  return Array.from({ length: gapNumber }).map((_section, index) => {
    const isEven = index % 2 === 0;
    const offset = 0.03; // 0.05;
    const startY = (index + 1) * gapHeight + startPoint.y / 4;
    const endY = (index + 2) * gapHeight + startPoint.y / 4;
    const controlPoint1X = isEven ? width * (1 - offset) : width * (0 + offset);
    const controlPoint1Y = startY + gapHeight / 2;
    const controlPoint2X = isEven ? width * (0 + offset) : width * (1 - offset);
    const controlPoint2Y = startY + gapHeight / 2;

    return `C ${controlPoint1X} ${controlPoint1Y}, ${controlPoint2X} ${controlPoint2Y}, ${controlPoint2X} ${endY}`;
  });
};

const drawPath = (styles, id) => {
  const svg = document.getElementById("timeline");
  const path = document.createElementNS("http://www.w3.org/2000/svg", "path");
  if (id) {
    path.setAttribute("id", id);
  }
  Object.keys(styles).forEach((key) => {
    path.setAttribute(key, styles[key]);
  });
  svg.appendChild(path);
};

const drawPaths = () => {
  const svg = document.getElementById("timeline");
  const { width } = svg.getBoundingClientRect();
  const startPointDOM = document.querySelector("#start-point");
  const endPointDOM = document.getElementById("s-35");
  const featureDOM = document.getElementById("s-37");
  const lineWidth = innerWidth < 740 ? 4 : 8;
 

  const journeyPath = {
    startPoint: {
      x: width * 0.5,
      y: calcPointY(startPointDOM) + 20,
    },
    endPoint: {
      x: width,
      y: calcPointY(endPointDOM),
    },
  };
  const feturePath = {
    startPoint: journeyPath.endPoint,
    endPoint: {
      x: width / 4,
      y: calcPointY(featureDOM),
    },
  };
  const totalHeight = feturePath.endPoint.y - 0;

  /** 设置画布的高度 */
  svg.setAttribute("height", totalHeight);
  const gapHeight = innerHeight * 0.6;
  const height = feturePath.endPoint.y - journeyPath.startPoint.y;
  const gapNumber = Math.round(height / gapHeight);

  /** normal */

  const paths = getPaths(journeyPath.startPoint, feturePath.endPoint, {
    width,
    gapHeight,
  });

  const first_break_point = paths[1].split(",")[2];

  drawPath(
    {
      d:
        `M ${journeyPath.startPoint.x} ${journeyPath.startPoint.y}` +
        paths.slice(0, 2).join(""),
      stroke: primaryColor,
      fill: "transparent",
      "stroke-width": lineWidth,
      "stroke-dasharray": "30,10",
    },
    "first-path"
  );

  drawPath({
    d: `M ${first_break_point}` + paths.slice(2, gapNumber - 2).join(""),
    stroke: primaryColor,
    "fill-opacity": "0.1",
    fill: "transparent",
    "stroke-width": lineWidth,
  });

  const last_point = paths[gapNumber - 3].split(",")[2];

  drawPath({
    d: `M ${last_point}` + paths.slice(gapNumber - 2, gapNumber).join(""),
    stroke: "#ddd",
    fill: "transparent",
    "stroke-width": lineWidth,
    "stroke-dasharray": "30,10",
  });
};
clearPaths = () => {
  const svgElement = document.getElementById("timeline");
  const paths = svgElement.querySelectorAll("path");
  paths.forEach((path) => path.remove());
};

/** 当元素出现在视窗的时候，触发的展示动画 */
const visibleAnimate = (targets, { visiableClass, hiddenClass }) => {
  const observer = new IntersectionObserver(
    (entries) => {
      entries.forEach((entry) => {
        const delay = entry.target.getAttribute("data-delay") || "0s";
        const duration = entry.target.getAttribute("data-duration") || "1s";
        if (entry.isIntersecting) {
          entry.target.classList.add(...visiableClass.split(" "));
          entry.target.classList.remove(...hiddenClass.split(" "));
          // entry.target.style.transition = `all ${duration} ease ${delay}`;
        } else {
          entry.target.classList.remove(...visiableClass.split(" "));
          entry.target.classList.add(...hiddenClass.split(" "));
          // entry.target.style.transition =`all 0.2s ease`;
        }
      });
    },
    { threshold: 0.1 }
  );

  targets.forEach((target) => observer.observe(target));
};




/** 针对 A->B 变化的元素（图片） */
const hl_images = document.querySelectorAll(".hl-image");
const animate_targets = document.querySelectorAll(".animate");
/** 针对 highlight 强调元素 */
const hl_targets = document.querySelectorAll(".hl");


const doDelay  = ()=>{
  const cls = "animate visible no-delay"
  animate_targets.forEach((target) => {
    target.classList.add(...cls.split(" "));
  });
  hl_images.forEach((target) => {
    target.classList.add(...cls.split(" "));
  });
  hl_targets.forEach((target) => {
    target.classList.add(...cls.split(" "));
  });
}


/** 截图的时候，执行该函数 */
// doDelay()


visibleAnimate(hl_images, {
  visiableClass: "animate-x",
  hiddenClass: "animate-x-hidden",
});

visibleAnimate(animate_targets, {
  visiableClass: "visible",
  hiddenClass: "hidden",
});

visibleAnimate(hl_targets, {
  visiableClass: "visible",
  hiddenClass: "hidden",
});

function debounce(func, wait) {
  let timeout;
  return function (...args) {
    const context = this;
    clearTimeout(timeout);
    timeout = setTimeout(() => func.apply(context, args), wait);
  };
}

const handleResize = () => {
  clearPaths();
  drawPaths();
};

window.addEventListener("resize", debounce(handleResize, 200));

/** 语言切换 */
function switchLanguage() {
  const lang = document.documentElement.lang;
  const newHref = lang === "en" ? "index.zh-CN.html" : "index.html";
  window.location.href = newHref;
}
const languageDOM = document.getElementById("language");
languageDOM.onclick = switchLanguage;

window.onload = () => {
  drawPaths();
};

