package com.btcsignal.watch;

import android.Manifest;
import android.app.Activity;
import android.app.AlertDialog;
import android.content.Context;
import android.content.Intent;
import android.content.SharedPreferences;
import android.content.pm.PackageManager;
import android.graphics.Color;
import android.net.Uri;
import android.os.Build;
import android.os.Bundle;
import android.os.PowerManager;
import android.provider.Settings;
import android.view.Gravity;
import android.view.View;
import android.webkit.JavascriptInterface;
import android.webkit.JsResult;
import android.webkit.WebChromeClient;
import android.webkit.WebResourceError;
import android.webkit.WebResourceRequest;
import android.webkit.WebSettings;
import android.webkit.WebView;
import android.webkit.WebViewClient;
import android.widget.FrameLayout;
import android.widget.TextView;

import java.net.HttpURLConnection;
import java.net.URL;

public class MainActivity extends Activity {
    private static final String APP_URL = "http://127.0.0.1:8080/";
    private static final int BG = Color.rgb(4, 7, 10);

    // Faz o Notification do navegador virar notificação nativa do Android
    private static final String POLYFILL =
            "(function(){if(window.__btcPoly)return;window.__btcPoly=true;"
          + "function N(t,o){try{AndroidBridge.push(String(t),String((o&&o.body)||''));}catch(e){}this.close=function(){};}"
          + "N.permission='granted';N.requestPermission=function(){return Promise.resolve('granted');};"
          + "window.Notification=N;"
          + "try{Object.defineProperty(navigator,'serviceWorker',{get:function(){return undefined;},configurable:true});}catch(e){}"
          + "})();";

    private WebView web;
    private TextView overlay;
    private volatile boolean alive = true;
    private volatile boolean loaded = false;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        getWindow().setStatusBarColor(BG);
        getWindow().setNavigationBarColor(BG);

        Notifier.init(getApplicationContext());

        FrameLayout root = new FrameLayout(this);
        root.setBackgroundColor(BG);

        web = new WebView(this);
        web.setBackgroundColor(BG);
        WebSettings s = web.getSettings();
        s.setJavaScriptEnabled(true);
        s.setDomStorageEnabled(true);
        s.setMediaPlaybackRequiresUserGesture(false);
        s.setMixedContentMode(WebSettings.MIXED_CONTENT_ALWAYS_ALLOW);
        web.addJavascriptInterface(new Bridge(), "AndroidBridge");

        web.setWebViewClient(new WebViewClient() {
            @Override
            public void onPageFinished(WebView view, String url) {
                if (url != null && url.startsWith("http://127.0.0.1")) {
                    view.evaluateJavascript(POLYFILL, null);
                    overlay.setVisibility(View.GONE);
                }
            }

            @Override
            public void onReceivedError(WebView view, WebResourceRequest request, WebResourceError error) {
                if (request != null && request.isForMainFrame()) {
                    showOverlay("Reconectando ao servidor…");
                    loaded = false;
                    waitForServer();
                }
            }

            @Override
            public boolean shouldOverrideUrlLoading(WebView view, WebResourceRequest request) {
                Uri u = request.getUrl();
                if ("127.0.0.1".equals(u.getHost())) return false;
                try {
                    startActivity(new Intent(Intent.ACTION_VIEW, u));
                } catch (Exception ignored) {
                }
                return true;
            }
        });

        web.setWebChromeClient(new WebChromeClient() {
            @Override
            public boolean onJsAlert(WebView view, String url, String message, final JsResult result) {
                new AlertDialog.Builder(MainActivity.this)
                        .setMessage(message)
                        .setPositiveButton("OK", (d, w) -> result.confirm())
                        .setOnCancelListener(d -> result.cancel())
                        .show();
                return true;
            }

            @Override
            public boolean onJsConfirm(WebView view, String url, String message, final JsResult result) {
                new AlertDialog.Builder(MainActivity.this)
                        .setMessage(message)
                        .setPositiveButton("OK", (d, w) -> result.confirm())
                        .setNegativeButton("Cancelar", (d, w) -> result.cancel())
                        .setOnCancelListener(d -> result.cancel())
                        .show();
                return true;
            }
        });

        overlay = new TextView(this);
        overlay.setBackgroundColor(BG);
        overlay.setTextColor(Color.rgb(18, 229, 138));
        overlay.setTextSize(20);
        overlay.setGravity(Gravity.CENTER);

        root.addView(web, new FrameLayout.LayoutParams(-1, -1));
        root.addView(overlay, new FrameLayout.LayoutParams(-1, -1));
        setContentView(root);
        showOverlay("₿  BTC SIGNAL WATCH\n\nIniciando servidor…");

        askPermissions();
        startForegroundService(new Intent(this, ServerService.class));
        waitForServer();
    }

    private void showOverlay(String text) {
        overlay.setText(text);
        overlay.setVisibility(View.VISIBLE);
    }

    private void askPermissions() {
        if (Build.VERSION.SDK_INT >= 33
                && checkSelfPermission(Manifest.permission.POST_NOTIFICATIONS) != PackageManager.PERMISSION_GRANTED) {
            requestPermissions(new String[]{Manifest.permission.POST_NOTIFICATIONS}, 1);
        }
        SharedPreferences sp = getSharedPreferences("btc", MODE_PRIVATE);
        PowerManager pm = (PowerManager) getSystemService(Context.POWER_SERVICE);
        if (!sp.getBoolean("askedBattery", false) && pm != null
                && !pm.isIgnoringBatteryOptimizations(getPackageName())) {
            sp.edit().putBoolean("askedBattery", true).apply();
            try {
                startActivity(new Intent(Settings.ACTION_REQUEST_IGNORE_BATTERY_OPTIMIZATIONS,
                        Uri.parse("package:" + getPackageName())));
            } catch (Exception ignored) {
            }
        }
    }

    private void waitForServer() {
        new Thread(() -> {
            while (alive && !loaded) {
                if (ping()) {
                    runOnUiThread(() -> {
                        if (alive && !loaded) {
                            loaded = true;
                            web.loadUrl(APP_URL);
                        }
                    });
                    return;
                }
                try {
                    Thread.sleep(700);
                } catch (InterruptedException e) {
                    return;
                }
            }
        }, "wait-server").start();
    }

    private boolean ping() {
        HttpURLConnection c = null;
        try {
            c = (HttpURLConnection) new URL(APP_URL + "health").openConnection();
            c.setConnectTimeout(800);
            c.setReadTimeout(1500);
            return c.getResponseCode() == 200;
        } catch (Exception e) {
            return false;
        } finally {
            if (c != null) c.disconnect();
        }
    }

    @Override
    public void onBackPressed() {
        if (web != null && web.canGoBack()) {
            web.goBack();
        } else {
            moveTaskToBack(true); // mantém o servidor rodando
        }
    }

    @Override
    protected void onDestroy() {
        alive = false;
        if (web != null) web.destroy();
        super.onDestroy();
    }

    private static class Bridge {
        @JavascriptInterface
        public void push(String title, String body) {
            Notifier.show(title, body);
        }
    }
}
