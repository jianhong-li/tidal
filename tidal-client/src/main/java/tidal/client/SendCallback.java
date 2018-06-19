package tidal.client;


public interface SendCallback {

	public void onSuccess(final SendResult sendResult);

    public void onException(final Throwable e);
}
